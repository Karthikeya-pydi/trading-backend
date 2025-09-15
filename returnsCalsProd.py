"""
Production Returns Calculation Flow

A streamlined production system for:
1. Downloading H5 data from S3
2. Converting H5 to CSV format
3. Calculating stock returns and scores
4. Uploading results to S3

S3 Structure:
- Input: parquet-eq-data/nse_data/[LATEST_H5_FILE] (dynamically selected)
- Output: trading-platform-csvs/adjusted-eq-data/adjusted-eq-data-YYYY-MM-DD.csv
"""

import boto3
import pandas as pd
import numpy as np
import io
import tempfile
import os
from datetime import datetime, timedelta
from typing import Optional, Dict
import logging
import warnings
warnings.filterwarnings('ignore')

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not installed, use system environment variables

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('returns_calculation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ProductionReturnsCalculator:
    """Production-ready returns calculator with S3 integration"""
    
    def __init__(self, input_bucket: str, output_bucket: str, h5_key: str, output_prefix: str,
                 input_credentials: dict = None, output_credentials: dict = None):
        self.input_bucket = input_bucket
        self.output_bucket = output_bucket
        self.h5_key = h5_key
        self.output_prefix = output_prefix
        self.data = None
        self.returns_data = None
        
        # Initialize S3 clients
        self._init_s3_clients(input_credentials, output_credentials)
        
    def _init_s3_clients(self, input_credentials: dict, output_credentials: dict):
        """Initialize S3 clients for input and output buckets"""
        try:
            # Input bucket client
            if input_credentials:
                self.input_s3_client = boto3.client(
                    's3',
                    aws_access_key_id=input_credentials['access_key'],
                    aws_secret_access_key=input_credentials['secret_key']
                )
            else:
                self.input_s3_client = boto3.client('s3')
            
            # Output bucket client
            if output_credentials:
                self.output_s3_client = boto3.client(
                    's3',
                    aws_access_key_id=output_credentials['access_key'],
                    aws_secret_access_key=output_credentials['secret_key']
                )
            else:
                self.output_s3_client = boto3.client('s3')
            
            # Test connections
            self.input_s3_client.head_bucket(Bucket=self.input_bucket)
            self.output_s3_client.head_bucket(Bucket=self.output_bucket)
            logger.info(f"Connected to S3: {self.input_bucket} (input), {self.output_bucket} (output)")
            
        except Exception as e:
            logger.error(f"S3 connection failed: {str(e)}")
            raise
    
    def download_and_convert_data(self) -> pd.DataFrame:
        """Download H5 data from S3 and convert to DataFrame"""
        try:
            logger.info("Downloading and converting H5 data...")
            
            # Download H5 data
            response = self.input_s3_client.get_object(Bucket=self.input_bucket, Key=self.h5_key)
            h5_data = response['Body'].read()
            logger.info(f"Downloaded H5 file ({len(h5_data)} bytes)")
            
            # Convert H5 to DataFrame
            self.data = self._convert_h5_to_dataframe(h5_data)
            logger.info(f"Converted to DataFrame: {self.data.shape}")
            
            return self.data
            
        except Exception as e:
            logger.error(f"Data download/conversion failed: {str(e)}")
            raise
    
    def _convert_h5_to_dataframe(self, h5_data: bytes) -> pd.DataFrame:
        """Convert H5 data to DataFrame using proven method"""
        with tempfile.NamedTemporaryFile(suffix='.h5', delete=False) as temp_file:
            temp_file.write(h5_data)
            temp_file_path = temp_file.name
        
        try:
            import h5py
            
            with h5py.File(temp_file_path, 'r') as f:
                # Try pandas read first
                try:
                    return pd.read_hdf(temp_file_path)
                except:
                    pass
                
                # Manual reconstruction for complex HDF5 structures
                if 'stage' in f:
                    stage = f['stage']
                    
                    # Get column names from block items
                    columns = []
                    for key in stage.keys():
                        if 'items' in key:
                            items = stage[key][:]
                            if items.dtype.kind == 'S':
                                items = [item.decode('utf-8') for item in items]
                            columns.extend(items)
                    
                    # Get data values
                    data_blocks = []
                    for key in stage.keys():
                        if 'values' in key:
                            values = stage[key][:]
                            if values.ndim == 2:
                                data_blocks.append(values)
                            elif values.ndim == 1:
                                data_blocks.append(values.reshape(-1, 1))
                    
                    if data_blocks:
                        combined_data = np.hstack(data_blocks)
                        return pd.DataFrame(combined_data, columns=columns)
                    else:
                        raise ValueError("No data blocks found")
                else:
                    raise ValueError("No 'stage' group found in HDF5 file")
        
        finally:
            os.unlink(temp_file_path)
    
    def calculate_returns(self, target_date: Optional[str] = None) -> pd.DataFrame:
        """Calculate returns for all symbols"""
        try:
            if self.data is None:
                raise ValueError("No data available. Run download_and_convert_data() first.")
            
            logger.info("Calculating returns...")
            
            # Determine target date
            if target_date is None:
                target_date = self.data['Date'].max().strftime('%Y-%m-%d')
            
            # Filter data for target date
            target_date_dt = pd.to_datetime(target_date)
            symbols_on_target_date = self.data[self.data['Date'] == target_date_dt]['Symbol'].unique()
            
            if len(symbols_on_target_date) == 0:
                raise ValueError(f"No stocks found with data on {target_date}")
            
            # Filter data to only include symbols available on target date
            filtered_data = self.data[self.data['Symbol'].isin(symbols_on_target_date)]
            
            # Calculate returns for each symbol
            results = []
            periods = {
                '1_Week': 7, '1_Month': 30, '3_Months': 90, '6_Months': 180,
                '9_Months': 270, '1_Year': 365, '3_Years': 1095, '5_Years': 1825
            }
            
            for symbol, group in filtered_data.groupby('Symbol'):
                if len(group) < 2:
                    continue
                
                try:
                    returns = self._calculate_symbol_returns(group, periods)
                    turnover = self._calculate_turnover(group)
                    latest_data = group[group['Date'] == group['Date'].max()].iloc[0]
                    
                    result = {
                        'Symbol': symbol,
                        'Fincode': latest_data.get('Fincode', ''),
                        'ISIN': latest_data.get('ISIN', ''),
                        'Latest_Date': latest_data['Date'],
                        'Latest_Close': latest_data['Close'],
                        'Latest_Volume': latest_data.get('Volume', 0),
                        'Turnover': turnover
                    }
                    result.update(returns)
                    results.append(result)
                    
                except Exception as e:
                    logger.warning(f"Error processing {symbol}: {str(e)}")
                    continue
            
            self.returns_data = pd.DataFrame(results)
            logger.info(f"Calculated returns for {len(self.returns_data)} symbols")
            return self.returns_data
            
        except Exception as e:
            logger.error(f"Returns calculation failed: {str(e)}")
            raise
    
    def calculate_stock_scores(self) -> pd.DataFrame:
        """Calculate raw and normalized scores for stocks using percentile normalization"""
        try:
            if self.returns_data is None:
                raise ValueError("No returns data available. Run calculate_returns() first.")
            
            logger.info("Calculating stock scores using percentile normalization...")
            
            # Define weights
            weights = {
                '1_Month': -0.10, '3_Months': 0.25, '6_Months': 0.25,
                '9_Months': 0.40, '1_Year': 0.20
            }
            
            # Calculate raw scores - work with available data only
            raw_scores = []
            missing_data_count = 0
            for _, row in self.returns_data.iterrows():
                available_columns = [col for col in weights.keys() if pd.notna(row[col])]
                missing_columns = [col for col in weights.keys() if pd.isna(row[col])]
                
                if len(available_columns) > 0:
                    # Calculate weighted sum using only available data
                    weighted_sum = sum(row[col] * weights[col] for col in available_columns)
                    
                    # Normalize by the sum of weights for available columns
                    total_weight = sum(weights[col] for col in available_columns)
                    normalized_score = (weighted_sum / total_weight) * 100 if total_weight != 0 else 0
                    
                    raw_scores.append(normalized_score)
                    
                    # Log missing data for first few stocks (for debugging)
                    if missing_columns and missing_data_count < 5:
                        logger.info(f"Stock {row['Symbol']}: Missing data for {missing_columns}, using {available_columns}")
                        missing_data_count += 1
                else:
                    raw_scores.append(np.nan)
            
            self.returns_data['Raw_Score'] = raw_scores
            
            # Calculate normalized scores using percentile normalization (1st-99th percentile)
            valid_scores = self.returns_data['Raw_Score'].dropna()
            if len(valid_scores) > 0:
                p1, p99 = valid_scores.quantile([0.01, 0.99])
                if p99 != p1:
                    normalized_scores = ((self.returns_data['Raw_Score'] - p1) / (p99 - p1)) * 100
                    normalized_scores = np.clip(normalized_scores, 0, 100)
                else:
                    normalized_scores = pd.Series([50.0] * len(self.returns_data), index=self.returns_data.index)
                
                self.returns_data['Normalized_Score'] = normalized_scores
            else:
                self.returns_data['Normalized_Score'] = np.nan
            
            # Log data availability summary
            data_availability = {}
            for col in weights.keys():
                if col in self.returns_data.columns:
                    available_count = self.returns_data[col].notna().sum()
                    total_count = len(self.returns_data)
                    data_availability[col] = f"{available_count}/{total_count} ({available_count/total_count*100:.1f}%)"
            
            logger.info("Data availability by period:")
            for period, availability in data_availability.items():
                logger.info(f"  {period}: {availability}")
            
            logger.info(f"Calculated scores for {len(valid_scores)} stocks")
            return self.returns_data
            
        except Exception as e:
            logger.error(f"Score calculation failed: {str(e)}")
            raise
    
    def _calculate_symbol_returns(self, symbol_data: pd.DataFrame, periods: Dict[str, int]) -> Dict[str, float]:
        """Calculate returns for a specific symbol"""
        if len(symbol_data) < 2:
            return {period: np.nan for period in periods.keys()}
        
        latest_date = symbol_data['Date'].max()
        latest_price = symbol_data[symbol_data['Date'] == latest_date]['Close'].iloc[0]
        
        returns = {}
        for period_name, days in periods.items():
            try:
                target_date = latest_date - timedelta(days=days)
                available_dates = symbol_data['Date'].sort_values()
                target_data = available_dates[available_dates <= target_date]
                
                if len(target_data) > 0:
                    closest_date = target_data.iloc[-1]
                    historical_price = symbol_data[symbol_data['Date'] == closest_date]['Close'].iloc[0]
                    
                    if historical_price > 0:
                        returns[period_name] = ((latest_price - historical_price) / historical_price) * 100
                    else:
                        returns[period_name] = np.nan
                else:
                    returns[period_name] = np.nan
            except:
                returns[period_name] = np.nan
                
        return returns
    
    def _calculate_turnover(self, symbol_data: pd.DataFrame) -> float:
        """Calculate turnover for a specific symbol"""
        six_months_days = 180
        
        if len(symbol_data) < six_months_days:
            return np.nan
        
        latest_date = symbol_data['Date'].max()
        current_volume = symbol_data[symbol_data['Date'] == latest_date]['Volume'].iloc[0]
        
        historical_data = symbol_data[symbol_data['Date'] < latest_date].tail(six_months_days)
        
        if len(historical_data) < six_months_days:
            return np.nan
        
        avg_close_price = historical_data['Close'].mean()
        return avg_close_price * current_volume
    
    def save_and_upload_results(self, target_date: Optional[str] = None) -> str:
        """Save results to CSV and upload to S3"""
        try:
            if self.returns_data is None:
                raise ValueError("No returns data available. Run calculate_returns() first.")
            
            # Determine target date for filename
            if target_date is None:
                target_date = self.returns_data['Latest_Date'].max().strftime('%Y-%m-%d')
            
            # Create S3 key
            s3_key = f"{self.output_prefix}/adjusted-eq-data-{target_date}.csv"
            
            # Convert DataFrame to CSV string and upload
            csv_data = self.returns_data.to_csv(index=False)
            self.output_s3_client.put_object(
                Bucket=self.output_bucket,
                Key=s3_key,
                Body=csv_data,
                ContentType='text/csv'
            )
            
            logger.info(f"Results uploaded to: s3://{self.output_bucket}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Save/upload failed: {str(e)}")
            raise
    
    def run_complete_flow(self, target_date: Optional[str] = None, include_scoring: bool = True) -> str:
        """Run the complete production flow"""
        try:
            logger.info("Starting production returns calculation flow...")
            
            # Step 1: Download and convert data
            self.download_and_convert_data()
            
            # Step 2: Calculate returns
            self.calculate_returns(target_date)
            
            # Step 3: Calculate scores (if requested)
            if include_scoring:
                self.calculate_stock_scores()
            
            # Step 4: Save and upload results
            s3_key = self.save_and_upload_results(target_date)
            
            logger.info("Flow completed successfully!")
            return s3_key
            
        except Exception as e:
            logger.error(f"Flow failed: {str(e)}")
            raise
    
    def display_summary(self):
        """Display a summary of the results"""
        if self.returns_data is None:
            logger.info("No results to display.")
            return
        
        print("\n" + "="*60)
        print("STOCK RETURNS & SCORING SUMMARY")
        print("="*60)
        
        print(f"Total symbols processed: {len(self.returns_data)}")
        print(f"Latest data date: {self.returns_data['Latest_Date'].max()}")
        
        # Display scoring summary if available
        if 'Raw_Score' in self.returns_data.columns:
            valid_scores = self.returns_data['Raw_Score'].dropna()
            if len(valid_scores) > 0:
                print(f"\nScoring Summary:")
                print(f"  Stocks with scores: {len(valid_scores)}")
                print(f"  Raw Score - Mean: {valid_scores.mean():.2f}, Range: {valid_scores.min():.2f} to {valid_scores.max():.2f}")
                
                if 'Normalized_Score' in self.returns_data.columns:
                    norm_scores = self.returns_data['Normalized_Score'].dropna()
                    print(f"  Normalized Score - Mean: {norm_scores.mean():.2f}, Range: {norm_scores.min():.2f} to {norm_scores.max():.2f}")
                
                # Top 5 performers
                print(f"\nTop 5 Stocks by Raw Score:")
                top_stocks = self.returns_data.nlargest(5, 'Raw_Score')[['Symbol', 'Raw_Score', 'Normalized_Score']]
                for _, row in top_stocks.iterrows():
                    print(f"  {row['Symbol']}: Raw={row['Raw_Score']:.2f}, Norm={row['Normalized_Score']:.2f}")


def main():
    """Main function to run the production flow"""
    try:
        # Configuration
        INPUT_BUCKET = "parquet-eq-data"
        OUTPUT_BUCKET = "trading-platform-csvs"
        H5_KEY = "nse_data/Our_Nseadjprice.h5"
        OUTPUT_PREFIX = "adjusted-eq-data"
        
        # AWS Credentials (from environment variables)
        input_credentials = {
            'access_key': os.getenv('INPUT_AWS_ACCESS_KEY_ID'),
            'secret_key': os.getenv('INPUT_AWS_SECRET_ACCESS_KEY')
        }
        
        output_credentials = {
            'access_key': os.getenv('AWS_ACCESS_KEY_ID'),
            'secret_key': os.getenv('AWS_SECRET_ACCESS_KEY')
        }
        
        # Create calculator and run flow
        calculator = ProductionReturnsCalculator(
            input_bucket=INPUT_BUCKET,
            output_bucket=OUTPUT_BUCKET,
            h5_key=H5_KEY,
            output_prefix=OUTPUT_PREFIX,
            input_credentials=input_credentials,
            output_credentials=output_credentials
        )
        
        # Run complete flow
        result_s3_key = calculator.run_complete_flow(include_scoring=True)
        
        # Display summary
        calculator.display_summary()
        
        print(f"\n✅ SUCCESS!")
        print(f"📁 Results: s3://{OUTPUT_BUCKET}/{result_s3_key}")
        print(f"📊 Includes: Returns, Turnover, Raw Scores, Normalized Scores")
        
    except Exception as e:
        logger.error(f"Production flow failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()