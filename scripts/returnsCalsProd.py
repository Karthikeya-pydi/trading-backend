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
        
        # Define exclusion patterns
        self.exclusion_patterns = [
            "ETF", "BEES", "NIFTY", "GOLD", "GLD", "SILVER", "SILV"
        ]
        
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
    
    def _should_exclude_symbol(self, symbol: str) -> bool:
        """Check if a symbol should be excluded based on exclusion patterns"""
        if pd.isna(symbol) or symbol == '':
            return True
        
        symbol_upper = str(symbol).upper()
        for pattern in self.exclusion_patterns:
            if pattern.upper() in symbol_upper:
                return True
        return False
    
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
            
            # Apply exclusion filter
            original_count = len(self.data)
            self.data = self._apply_exclusion_filter(self.data)
            excluded_count = original_count - len(self.data)
            logger.info(f"Applied exclusion filter: {excluded_count} symbols excluded, {len(self.data)} remaining")
            
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
    
    def _apply_exclusion_filter(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply exclusion filter to remove unwanted symbols"""
        try:
            logger.info("Applying exclusion filter...")
            
            # Log exclusion patterns
            logger.info(f"Exclusion patterns: {self.exclusion_patterns}")
            
            # Get unique symbols before filtering
            original_symbols = data['Symbol'].unique() if 'Symbol' in data.columns else []
            logger.info(f"Original symbols count: {len(original_symbols)}")
            
            # Apply exclusion filter
            if 'Symbol' in data.columns:
                # Create exclusion mask
                exclusion_mask = data['Symbol'].apply(self._should_exclude_symbol)
                
                # Log excluded symbols
                excluded_symbols = data[exclusion_mask]['Symbol'].unique()
                if len(excluded_symbols) > 0:
                    logger.info(f"Excluded symbols: {excluded_symbols[:10]}{'...' if len(excluded_symbols) > 10 else ''}")
                
                # Filter data
                filtered_data = data[~exclusion_mask]
                
                logger.info(f"Exclusion filter applied: {len(data)} -> {len(filtered_data)} records")
                logger.info(f"Symbols excluded: {len(excluded_symbols)}")
                
                return filtered_data
            else:
                logger.warning("No 'Symbol' column found, skipping exclusion filter")
                return data
                
        except Exception as e:
            logger.error(f"Exclusion filter failed: {str(e)}")
            return data
    
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
            fincodes_on_target_date = self.data[self.data['Date'] == target_date_dt]['Fincode'].unique()
            
            if len(fincodes_on_target_date) == 0:
                raise ValueError(f"No stocks found with data on {target_date}")
            
            # Filter data to only include fincodes available on target date
            filtered_data = self.data[self.data['Fincode'].isin(fincodes_on_target_date)]
            
            # Calculate returns for each fincode
            results = []
            periods = {
                '1_Week': 7, '1_Month': 30, '3_Months': 90, '6_Months': 180,
                '9_Months': 270, '1_Year': 365, '3_Years': 1095, '5_Years': 1825
            }
            
            for fincode, group in filtered_data.groupby('Fincode'):
                if len(group) < 2:
                    continue
                
                try:
                    returns = self._calculate_symbol_returns(group, periods)
                    turnover = self._calculate_turnover(group)
                    latest_data = group[group['Date'] == group['Date'].max()].iloc[0]
                    
                    result = {
                        'Fincode': fincode,
                        'Symbol': latest_data.get('Symbol', ''),
                        'ISIN': latest_data.get('ISIN', ''),
                        'Latest_Date': latest_data['Date'],
                        'Latest_Close': latest_data['Close'],
                        'Latest_Volume': latest_data.get('Volume', 0),
                        'Turnover': turnover
                    }
                    result.update(returns)
                    results.append(result)
                    
                except Exception as e:
                    logger.warning(f"Error processing {fincode}: {str(e)}")
                    continue
            
            self.returns_data = pd.DataFrame(results)
            logger.info(f"Calculated returns for {len(self.returns_data)} fincodes")
            return self.returns_data
            
        except Exception as e:
            logger.error(f"Returns calculation failed: {str(e)}")
            raise
    
    def calculate_stock_scores(self) -> pd.DataFrame:
        """Calculate raw scores for stocks"""
        try:
            if self.returns_data is None:
                raise ValueError("No returns data available. Run calculate_returns() first.")
            
            logger.info("Calculating stock scores...")
            
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
                    weighted_sum = 0
                    total_weight = 0
                    
                    for col in available_columns:
                        if col == '1_Month':
                            # Special logic for 1-month: punish negative returns more, reward positive returns less
                            if row[col] < 0:  # Negative return - punish more
                                weight = 0.10  # Positive weight to amplify the negative
                            else:  # Positive return - reward less
                                weight = -0.10  # Negative weight to reduce the positive
                        else:
                            weight = weights[col]
                        
                        weighted_sum += row[col] * weight
                        total_weight += weight
                    
                    # Calculate raw score without normalization
                    raw_score = weighted_sum
                    
                    raw_scores.append(raw_score)
                    
                    # Log missing data for first few stocks (for debugging)
                    if missing_columns and missing_data_count < 5:
                        logger.info(f"Stock {row['Fincode']}: Missing data for {missing_columns}, using {available_columns}")
                        missing_data_count += 1
                else:
                    raw_scores.append(np.nan)
            
            self.returns_data['Raw_Score'] = raw_scores
            
            # No normalization - raw scores only
            
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
            
            logger.info(f"Calculated scores for {len(raw_scores)} stocks")
            
            # ADDITIONAL: Calculate historical raw scores
            logger.info("Calculating historical raw scores...")
            
            # Calculate historical scores for each stock
            historical_scores = self._calculate_historical_scores()
            
            # Add historical score columns to the dataframe
            for period in ['1_Week', '1_Month', '3_Months', '6_Months', '9_Months', '1_Year']:
                score_column = f"{period}_Raw_Score"
                self.returns_data[score_column] = historical_scores.get(period, np.nan)
                logger.info(f"Added {score_column} for historical scoring")
            
            logger.info(f"Completed weighted scoring and historical scoring")
            
            # ADDITIONAL: Calculate percentage changes in scores
            logger.info("Calculating percentage changes in scores...")
            self._calculate_score_percentage_changes()
            
            return self.returns_data
            
        except Exception as e:
            logger.error(f"Score calculation failed: {str(e)}")
            raise
    
    def _calculate_score_percentage_changes(self):
        """Calculate percentage changes in raw scores over different time periods"""
        try:
            logger.info("Calculating percentage changes in raw scores...")
            
            # Define historical periods
            historical_periods = ['1_Week', '1_Month', '3_Months', '6_Months', '9_Months', '1_Year']
            
            # Calculate percentage changes for each period
            for period in historical_periods:
                current_score_col = 'Raw_Score'
                historical_score_col = f"{period}_Raw_Score"
                # Convert period name to simpler format for column name
                period_simple = period.lower().replace('_', '')
                percentage_change_col = f"%change_{period_simple}"
                
                # Calculate percentage change: ((current - historical) / historical) * 100
                # Handle cases where historical score is 0 or NaN
                def calculate_percentage_change(current, historical):
                    if pd.isna(current) or pd.isna(historical) or historical == 0:
                        return np.nan
                    return ((current - historical) / abs(historical)) * 100
                
                # Apply percentage change calculation
                self.returns_data[percentage_change_col] = self.returns_data.apply(
                    lambda row: calculate_percentage_change(
                        row[current_score_col], 
                        row[historical_score_col]
                    ), axis=1
                )
                
                # Log statistics for this period
                valid_changes = self.returns_data[percentage_change_col].dropna()
                if len(valid_changes) > 0:
                    logger.info(f"{period} Score Change: {len(valid_changes)} stocks, "
                              f"Mean: {valid_changes.mean():.2f}%, "
                              f"Range: {valid_changes.min():.2f}% to {valid_changes.max():.2f}%")
                else:
                    logger.warning(f"No valid percentage changes calculated for {period}")
            
            logger.info("Completed percentage change calculations for all periods")
            
            # ADDITIONAL: Calculate sign comparison patterns
            logger.info("Calculating sign comparison patterns...")
            self._calculate_sign_comparisons()
            
        except Exception as e:
            logger.error(f"Percentage change calculation failed: {str(e)}")
            raise
    
    def _calculate_sign_comparisons(self):
        """Calculate sign comparison patterns between current and historical scores"""
        try:
            logger.info("Calculating sign comparison patterns...")
            
            # Define historical periods
            historical_periods = ['1_Week', '1_Month', '3_Months', '6_Months', '9_Months', '1_Year']
            
            # Calculate sign patterns for each period
            for period in historical_periods:
                current_score_col = 'Raw_Score'
                historical_score_col = f"{period}_Raw_Score"
                # Convert period name to simpler format for column name
                period_simple = period.lower().replace('_', '')
                sign_pattern_col = f"symbol_{period_simple}"
                
                # Calculate sign pattern: current_sign, historical_sign
                def calculate_sign_pattern(current, historical):
                    if pd.isna(current) or pd.isna(historical):
                        return np.nan
                    
                    current_sign = '+' if current >= 0 else '-'
                    historical_sign = '+' if historical >= 0 else '-'
                    
                    return f"{current_sign}, {historical_sign}"
                
                # Apply sign pattern calculation
                self.returns_data[sign_pattern_col] = self.returns_data.apply(
                    lambda row: calculate_sign_pattern(
                        row[current_score_col], 
                        row[historical_score_col]
                    ), axis=1
                )
                
                # Log statistics for this period
                valid_patterns = self.returns_data[sign_pattern_col].dropna()
                if len(valid_patterns) > 0:
                    pattern_counts = valid_patterns.value_counts()
                    logger.info(f"{period} Sign Patterns: {len(valid_patterns)} stocks")
                    for pattern, count in pattern_counts.items():
                        logger.info(f"  {pattern}: {count} stocks")
                else:
                    logger.warning(f"No valid sign patterns calculated for {period}")
            
            logger.info("Completed sign comparison calculations for all periods")
            
        except Exception as e:
            logger.error(f"Sign comparison calculation failed: {str(e)}")
            raise
    
    def _calculate_historical_scores(self) -> Dict[str, pd.Series]:
        """Calculate historical raw scores for each stock at different time points"""
        try:
            logger.info("Calculating historical raw scores...")
            
            # Get the latest date from current data
            latest_date = self.returns_data['Latest_Date'].max()
            logger.info(f"Latest date in data: {latest_date}")
            
            # Check available dates in the dataset
            all_dates = sorted(self.data['Date'].unique())
            logger.info(f"Available dates range: {all_dates[0]} to {all_dates[-1]} (total: {len(all_dates)} dates)")
            
            # Define historical periods
            historical_periods = {
                '1_Week': 7,
                '1_Month': 30, 
                '3_Months': 90,
                '6_Months': 180,
                '9_Months': 270,
                '1_Year': 365
            }
            
            historical_scores = {}
            
            for period_name, days_back in historical_periods.items():
                logger.info(f"Calculating scores for {period_name} ago...")
                
                # Calculate target date
                target_date = latest_date - timedelta(days=days_back)
                
                # Find the closest available date to target_date (within 5 days)
                available_dates = self.data['Date'].unique()
                available_dates = available_dates[available_dates <= target_date]
                
                if len(available_dates) == 0:
                    logger.warning(f"No data found before {target_date}, skipping {period_name}")
                    historical_scores[period_name] = pd.Series([np.nan] * len(self.returns_data), 
                                                               index=self.returns_data.index)
                    continue
                
                # Get the closest date (most recent before or on target_date)
                closest_date = available_dates.max()
                
                # Check if the closest date is within reasonable range (within 5 days)
                days_diff = (target_date - closest_date).days
                if days_diff > 5:
                    logger.warning(f"Closest available date {closest_date} is {days_diff} days away from target {target_date}, skipping {period_name}")
                    historical_scores[period_name] = pd.Series([np.nan] * len(self.returns_data), 
                                                               index=self.returns_data.index)
                    continue
                
                logger.info(f"Using closest date {closest_date} (target was {target_date}, diff: {days_diff} days)")
                
                # Get data for the closest date
                target_date_data = self.data[self.data['Date'] == closest_date]
                
                # Calculate returns for that historical date
                historical_returns = self._calculate_historical_returns(closest_date, target_date_data)
                
                if len(historical_returns) == 0:
                    logger.warning(f"No historical returns calculated for {period_name}")
                    historical_scores[period_name] = pd.Series([np.nan] * len(self.returns_data), 
                                                               index=self.returns_data.index)
                    continue
                
                # Use raw scores directly (no normalization)
                # Map historical scores to current stocks
                current_scores = []
                for _, current_stock in self.returns_data.iterrows():
                    fincode = current_stock['Fincode']
                    if fincode in historical_returns:
                        current_scores.append(historical_returns[fincode])
                    else:
                        current_scores.append(np.nan)
                
                historical_scores[period_name] = pd.Series(current_scores, index=self.returns_data.index)
                logger.info(f"Calculated {period_name} historical scores for {len([s for s in current_scores if not pd.isna(s)])} stocks")
            
            return historical_scores
            
        except Exception as e:
            logger.error(f"Historical score calculation failed: {str(e)}")
            raise
    
    def _calculate_historical_returns(self, target_date: datetime, target_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate returns for stocks as of a specific historical date"""
        try:
            historical_returns = {}
            
            # Define periods for historical calculation
            periods = {
                '1_Week': 7, '1_Month': 30, '3_Months': 90, '6_Months': 180,
                '9_Months': 270, '1_Year': 365, '3_Years': 1095, '5_Years': 1825
            }
            
            for fincode in target_data['Fincode'].unique():
                # Get all data for this fincode up to the target date
                fincode_data = self.data[(self.data['Fincode'] == fincode) & (self.data['Date'] <= target_date)]
                
                if len(fincode_data) < 2:
                    continue
                
                # Calculate returns for this fincode as of the target date
                returns = self._calculate_symbol_returns_as_of_date(fincode_data, periods, target_date)
                
                # Use the same weighted calculation as current scoring
                weights = {
                    '1_Month': -0.10, '3_Months': 0.25, '6_Months': 0.25,
                    '9_Months': 0.40, '1_Year': 0.20
                }
                
                # Calculate weighted score
                available_columns = [col for col in weights.keys() if pd.notna(returns.get(col, np.nan))]
                
                if len(available_columns) > 0:
                    # Calculate weighted sum using conditional logic for 1-month
                    weighted_sum = 0
                    total_weight = 0
                    
                    for col in available_columns:
                        if col == '1_Month':
                            # Special logic for 1-month: punish negative returns more, reward positive returns less
                            if returns[col] < 0:  # Negative return - punish more
                                weight = 0.10  # Positive weight to amplify the negative
                            else:  # Positive return - reward less
                                weight = -0.10  # Negative weight to reduce the positive
                        else:
                            weight = weights[col]
                        
                        weighted_sum += returns[col] * weight
                        total_weight += weight
                    
                    # Calculate raw score without normalization
                    raw_score = weighted_sum
                    historical_returns[fincode] = raw_score
            
            return historical_returns
            
        except Exception as e:
            logger.error(f"Historical returns calculation failed: {str(e)}")
            return {}
    
    def _calculate_symbol_returns_as_of_date(self, symbol_data: pd.DataFrame, periods: Dict[str, int], target_date: datetime) -> Dict[str, float]:
        """Calculate returns for a specific symbol as of a specific historical date"""
        if len(symbol_data) < 2:
            return {period: np.nan for period in periods.keys()}
        
        # Get the price on the target date (or closest available date)
        target_date_data = symbol_data[symbol_data['Date'] <= target_date]
        if len(target_date_data) == 0:
            return {period: np.nan for period in periods.keys()}
        
        latest_date_for_target = target_date_data['Date'].max()
        latest_price = target_date_data[target_date_data['Date'] == latest_date_for_target]['Close'].iloc[0]
        
        returns = {}
        for period_name, days in periods.items():
            try:
                historical_target_date = latest_date_for_target - timedelta(days=days)
                available_dates = symbol_data['Date'].sort_values()
                historical_data = available_dates[available_dates <= historical_target_date]
                
                if len(historical_data) > 0:
                    closest_date = historical_data.iloc[-1]
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
        """Calculate turnover for a specific symbol using last 6 months average"""
        from datetime import timedelta
        
        latest_date = symbol_data['Date'].max()
        current_volume = symbol_data[symbol_data['Date'] == latest_date]['Volume'].iloc[0]
        
        # Calculate 6 months ago from latest date
        six_months_ago = latest_date - timedelta(days=180)  # 6 months = ~180 days
        
        # Get historical data from 6 months ago to latest date (excluding latest)
        historical_data = symbol_data[
            (symbol_data['Date'] >= six_months_ago) & 
            (symbol_data['Date'] < latest_date)
        ]
        
        # Need at least 100 days of data for meaningful average
        if len(historical_data) < 100:
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
            s3_key = f"{self.output_prefix}/returns-{target_date}.csv"
            
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
        
        print(f"Total fincodes processed: {len(self.returns_data)}")
        print(f"Latest data date: {self.returns_data['Latest_Date'].max()}")
        print(f"Exclusion patterns applied: {self.exclusion_patterns}")
        
        # Display scoring summary if available
        if 'Raw_Score' in self.returns_data.columns:
            valid_scores = self.returns_data['Raw_Score'].dropna()
            if len(valid_scores) > 0:
                print(f"\nScoring Summary:")
                print(f"  Stocks with scores: {len(valid_scores)}")
                print(f"  Raw Score - Mean: {valid_scores.mean():.2f}, Range: {valid_scores.min():.2f} to {valid_scores.max():.2f}")
                
                # Top 5 performers
                print(f"\nTop 5 Stocks by Raw Score:")
                top_stocks = self.returns_data.nlargest(5, 'Raw_Score')[['Fincode', 'Symbol', 'Raw_Score']]
                for _, row in top_stocks.iterrows():
                    print(f"  {row['Fincode']} ({row['Symbol']}): Raw={row['Raw_Score']:.2f}")
        
        # Display historical raw scores and percentage changes
        historical_periods = ['1_Week', '1_Month', '3_Months', '6_Months', '9_Months', '1_Year']
        available_historical_columns = [f"{period}_Raw_Score" for period in historical_periods 
                                       if f"{period}_Raw_Score" in self.returns_data.columns]
        
        if available_historical_columns:
            print(f"\nHistorical Raw Scores Summary:")
            print(f"(Shows what each stock's raw score was at different points in the past)")
            for period in historical_periods:
                score_col = f"{period}_Raw_Score"
                if score_col in self.returns_data.columns:
                    valid_scores = self.returns_data[score_col].dropna()
                    if len(valid_scores) > 0:
                        print(f"  {period} ago: Mean={valid_scores.mean():.2f}, Range={valid_scores.min():.2f} to {valid_scores.max():.2f} ({len(valid_scores)} stocks)")
            
            # Display percentage changes summary
            print(f"\nScore Percentage Changes Summary:")
            print(f"(Shows how much each stock's score has changed compared to historical periods)")
            for period in historical_periods:
                period_simple = period.lower().replace('_', '')
                change_col = f"%change_{period_simple}"
                if change_col in self.returns_data.columns:
                    valid_changes = self.returns_data[change_col].dropna()
                    if len(valid_changes) > 0:
                        print(f"  vs {period} ago: Mean={valid_changes.mean():.2f}%, Range={valid_changes.min():.2f}% to {valid_changes.max():.2f}% ({len(valid_changes)} stocks)")
            
            # Display sign patterns summary
            print(f"\nSign Pattern Summary:")
            print(f"(Shows sign patterns: Current, Historical for each period)")
            for period in historical_periods:
                period_simple = period.lower().replace('_', '')
                sign_col = f"symbol_{period_simple}"
                if sign_col in self.returns_data.columns:
                    valid_patterns = self.returns_data[sign_col].dropna()
                    if len(valid_patterns) > 0:
                        pattern_counts = valid_patterns.value_counts()
                        print(f"  {period} patterns: {len(valid_patterns)} stocks")
                        for pattern, count in pattern_counts.items():
                            print(f"    {pattern}: {count} stocks")
            
            # Show sample of stocks with their historical scores, percentage changes, and sign patterns
            print(f"\nSample Stocks with Historical Scores, Percentage Changes, and Sign Patterns:")
            sample_data = self.returns_data[self.returns_data['Raw_Score'].notna()].head(5)
            for _, row in sample_data.iterrows():
                print(f"  {row['Fincode']} ({row['Symbol']}):")
                print(f"    Current Score: {row['Raw_Score']:.2f}")
                for period in historical_periods:
                    score_col = f"{period}_Raw_Score"
                    period_simple = period.lower().replace('_', '')
                    change_col = f"%change_{period_simple}"
                    sign_col = f"symbol_{period_simple}"
                    if score_col in self.returns_data.columns and pd.notna(row[score_col]):
                        # Format the historical score with symbol and %change
                        symbol_text = f" ({row['Symbol']})" if pd.notna(row.get('Symbol', '')) else ""
                        change_text = ""
                        if change_col in self.returns_data.columns and pd.notna(row[change_col]):
                            change_text = f" {row[change_col]:+.2f}%"
                        sign_text = ""
                        if sign_col in self.returns_data.columns and pd.notna(row[sign_col]):
                            sign_text = f" [{row[sign_col]}]"
                        print(f"    {period}_raw_score: {row[score_col]:.2f}{symbol_text}{change_text}{sign_text}")


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
        
        # Run complete flow with latest available date from data
        target_date = None  # Will automatically use the latest date from the data
        result_s3_key = calculator.run_complete_flow(target_date=target_date, include_scoring=True)
        
        # Display summary
        calculator.display_summary()
        
        print(f"\nSUCCESS! Production flow completed successfully!")
        print(f"Results: s3://{OUTPUT_BUCKET}/{result_s3_key}")
        print(f"Includes: Returns, Turnover, Current Scores, Historical Raw Scores (1 Week, 1 Month, 3 Months, 6 Months, 9 Months, 1 Year ago)")
        print(f"EXCLUSIONS APPLIED:")
        print(f"  - Symbols containing: ETF, BEES, NIFTY, GOLD/GLD, SILVER/SILV")
        print(f"NEW FEATURES:")
        print(f"  - Score Percentage Changes (%change_1week, %change_1month, etc.)")
        print(f"  - Sign Pattern Comparisons (symbol_1week, symbol_1month, etc.)")
        
    except Exception as e:
        logger.error(f"Production flow failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()