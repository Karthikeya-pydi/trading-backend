"""
Screener Data Enhancement Script

This script:
1. Downloads the latest adjusted-eq-data CSV from S3
2. Extracts Screener data (Market Cap, Sector, Industry, ROE, ROCE) for all stocks
3. Merges the data and uploads the enhanced file back to S3

Usage:
    python screenerDataEnhancer.py

S3 Structure:
- Input: trading-platform-csvs/adjusted-eq-data/adjusted-eq-data-YYYY-MM-DD.csv
- Output: trading-platform-csvs/adjusted-eq-data/adjusted-eq-data-enhanced-YYYY-MM-DD.csv
"""

import boto3
import pandas as pd
import numpy as np
import io
import os
from datetime import datetime
from typing import Optional, Dict, List
import logging
import warnings
import requests
import time
from bs4 import BeautifulSoup
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
        logging.FileHandler('screener_enhancement.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class ScreenerDataEnhancer:
    """Enhance existing returns data with Screener information"""
    
    def __init__(self, bucket_name: str, prefix: str, credentials: dict = None):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.s3_client = None
        self.returns_data = None
        self.screener_data = None
        
        # Initialize S3 client
        self._init_s3_client(credentials)
    
    def _init_s3_client(self, credentials: dict):
        """Initialize S3 client"""
        try:
            if credentials:
                self.s3_client = boto3.client(
                    's3',
                    aws_access_key_id=credentials['access_key'],
                    aws_secret_access_key=credentials['secret_key']
                )
            else:
                self.s3_client = boto3.client('s3')
            
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Connected to S3 bucket: {self.bucket_name}")
            
        except Exception as e:
            logger.error(f"S3 connection failed: {str(e)}")
            raise
    
    def _get_latest_returns_file(self) -> str:
        """Get the latest adjusted-eq-data file from S3"""
        try:
            logger.info("Finding latest returns file...")
            
            # List objects with the prefix
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.prefix
            )
            
            if 'Contents' not in response:
                raise ValueError(f"No files found with prefix: {self.prefix}")
            
            # Filter for CSV files and get the latest one
            csv_files = [obj for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            
            if not csv_files:
                raise ValueError(f"No CSV files found with prefix: {self.prefix}")
            
            # Sort by last modified date (most recent first)
            latest_file = max(csv_files, key=lambda x: x['LastModified'])
            latest_key = latest_file['Key']
            
            logger.info(f"Latest returns file: {latest_key}")
            return latest_key
            
        except Exception as e:
            logger.error(f"Failed to find latest returns file: {str(e)}")
            raise
    
    def download_returns_data(self) -> pd.DataFrame:
        """Download the latest returns data from S3"""
        try:
            latest_key = self._get_latest_returns_file()
            
            logger.info(f"Downloading returns data from: s3://{self.bucket_name}/{latest_key}")
            
            # Download the file
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=latest_key)
            csv_data = response['Body'].read().decode('utf-8')
            
            # Convert to DataFrame
            self.returns_data = pd.read_csv(io.StringIO(csv_data))
            logger.info(f"Downloaded returns data: {self.returns_data.shape}")
            
            return self.returns_data
            
        except Exception as e:
            logger.error(f"Failed to download returns data: {str(e)}")
            raise
    
    def _extract_screener_data(self, symbol: str) -> Dict[str, any]:
        """Extract financial data from Screener.in for a given symbol"""
        try:
            # Screener URL format
            url = f"https://www.screener.in/company/{symbol}/"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Initialize result dictionary
            result = {
                'Market_Cap': np.nan,
                'Sector': '',
                'Industry': '',
                'ROE': np.nan,
                'ROCE': np.nan
            }
            
            # Extract Market Cap - try multiple selectors
            try:
                market_cap_selectors = [
                    lambda soup: soup.find('span', string=lambda text: text and 'Market Cap' in text),
                    lambda soup: soup.find('div', string=lambda text: text and 'Market Cap' in text),
                    lambda soup: soup.find('td', string=lambda text: text and 'Market Cap' in text)
                ]
                
                for selector in market_cap_selectors:
                    market_cap_element = selector(soup)
                    if market_cap_element:
                        # Try to find the value in next sibling or parent
                        value_element = market_cap_element.find_next('span') or market_cap_element.find_next('td')
                        if value_element:
                            market_cap_text = value_element.get_text(strip=True)
                            # Convert to numeric value (remove Rs., Cr, etc.)
                            market_cap_text = market_cap_text.replace('Rs.', '').replace('Cr', '').replace(',', '').strip()
                            if market_cap_text.replace('.', '').replace('-', '').isdigit():
                                result['Market_Cap'] = float(market_cap_text)
                                break
            except:
                pass
            
            # Extract Sector and Industry
            try:
                sector_selectors = [
                    lambda soup: soup.find('span', string=lambda text: text and 'Sector' in text),
                    lambda soup: soup.find('div', string=lambda text: text and 'Sector' in text),
                    lambda soup: soup.find('td', string=lambda text: text and 'Sector' in text)
                ]
                
                for selector in sector_selectors:
                    sector_element = selector(soup)
                    if sector_element:
                        value_element = sector_element.find_next('span') or sector_element.find_next('td')
                        if value_element:
                            result['Sector'] = value_element.get_text(strip=True)
                            break
                
                industry_selectors = [
                    lambda soup: soup.find('span', string=lambda text: text and 'Industry' in text),
                    lambda soup: soup.find('div', string=lambda text: text and 'Industry' in text),
                    lambda soup: soup.find('td', string=lambda text: text and 'Industry' in text)
                ]
                
                for selector in industry_selectors:
                    industry_element = selector(soup)
                    if industry_element:
                        value_element = industry_element.find_next('span') or industry_element.find_next('td')
                        if value_element:
                            result['Industry'] = value_element.get_text(strip=True)
                            break
            except:
                pass
            
            # Extract ROE and ROCE from ratios section
            try:
                ratios_section = soup.find('div', {'id': 'ratios'}) or soup.find('section', string=lambda text: text and 'Ratios' in text)
                if ratios_section:
                    # Look for ROE
                    roe_element = ratios_section.find('span', string=lambda text: text and 'ROE' in text)
                    if roe_element:
                        roe_value = roe_element.find_next('span') or roe_element.find_next('td')
                        if roe_value:
                            roe_text = roe_value.get_text(strip=True)
                            if roe_text.replace('.', '').replace('%', '').replace('-', '').isdigit():
                                result['ROE'] = float(roe_text.replace('%', ''))
                    
                    # Look for ROCE
                    roce_element = ratios_section.find('span', string=lambda text: text and 'ROCE' in text)
                    if roce_element:
                        roce_value = roce_element.find_next('span') or roce_element.find_next('td')
                        if roce_value:
                            roce_text = roce_value.get_text(strip=True)
                            if roce_text.replace('.', '').replace('%', '').replace('-', '').isdigit():
                                result['ROCE'] = float(roce_text.replace('%', ''))
            except:
                pass
            
            return result
            
        except Exception as e:
            logger.warning(f"Failed to extract Screener data for {symbol}: {str(e)}")
            return {
                'Market_Cap': np.nan,
                'Sector': '',
                'Industry': '',
                'ROE': np.nan,
                'ROCE': np.nan
            }
    
    def extract_screener_data_for_all_stocks(self, batch_size: int = 50, delay_between_batches: int = 300) -> pd.DataFrame:
        """Extract Screener data for all stocks in the returns data"""
        if self.returns_data is None:
            raise ValueError("No returns data available. Run download_returns_data() first.")
        
        # Get unique symbols from returns data
        symbols = self.returns_data['Symbol'].unique().tolist()
        logger.info(f"Extracting Screener data for {len(symbols)} unique symbols")
        logger.info(f"Estimated time: {(len(symbols) * 2.0 + (len(symbols) // batch_size) * delay_between_batches) / 3600:.1f} hours")
        
        all_results = []
        total_batches = (len(symbols) + batch_size - 1) // batch_size
        successful_count = 0
        failed_count = 0
        
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min(start_idx + batch_size, len(symbols))
            batch_symbols = symbols[start_idx:end_idx]
            
            logger.info(f"Processing batch {batch_num + 1}/{total_batches}: symbols {start_idx}-{end_idx-1} ({len(batch_symbols)} symbols)")
            
            # Process this batch
            batch_results = []
            batch_successful = 0
            batch_failed = 0
            
            for i, symbol in enumerate(batch_symbols):
                try:
                    # Add delay between requests
                    time.sleep(2.0)
                    
                    screener_data = self._extract_screener_data(symbol)
                    screener_data['Symbol'] = symbol
                    batch_results.append(screener_data)
                    batch_successful += 1
                    successful_count += 1
                    
                    # Log progress within batch
                    if (i + 1) % 10 == 0:
                        logger.info(f"  Batch {batch_num + 1}: Processed {i + 1}/{len(batch_symbols)} symbols")
                        
                except Exception as e:
                    batch_failed += 1
                    failed_count += 1
                    logger.warning(f"Failed to process {symbol} in batch {batch_num + 1}: {str(e)}")
                    empty_data = {
                        'Symbol': symbol,
                        'Market_Cap': np.nan,
                        'Sector': '',
                        'Industry': '',
                        'ROE': np.nan,
                        'ROCE': np.nan
                    }
                    batch_results.append(empty_data)
            
            all_results.extend(batch_results)
            
            # Log batch completion
            logger.info(f"Batch {batch_num + 1} completed: {batch_successful} successful, {batch_failed} failed")
            logger.info(f"Overall progress: {successful_count} successful, {failed_count} failed out of {len(symbols)} total")
            
            # Add delay between batches (except for the last batch)
            if batch_num < total_batches - 1:
                logger.info(f"Waiting {delay_between_batches} seconds before next batch...")
                time.sleep(delay_between_batches)
        
        self.screener_data = pd.DataFrame(all_results)
        logger.info(f"COMPLETED Screener data extraction: {len(self.screener_data)} symbols processed")
        logger.info(f"Final results: {successful_count} successful, {failed_count} failed")
        
        return self.screener_data
    
    def merge_and_enhance_data(self) -> pd.DataFrame:
        """Merge returns data with Screener data"""
        if self.returns_data is None:
            raise ValueError("No returns data available. Run download_returns_data() first.")
        if self.screener_data is None:
            raise ValueError("No Screener data available. Run extract_screener_data_for_all_stocks() first.")
        
        logger.info("Merging returns data with Screener data...")
        
        # Merge on Symbol
        enhanced_data = self.returns_data.merge(
            self.screener_data[['Symbol', 'Market_Cap', 'Sector', 'Industry', 'ROE', 'ROCE']],
            on='Symbol',
            how='left'
        )
        
        # Fill missing values
        enhanced_data['Market_Cap'] = enhanced_data['Market_Cap'].fillna(np.nan)
        enhanced_data['Sector'] = enhanced_data['Sector'].fillna('')
        enhanced_data['Industry'] = enhanced_data['Industry'].fillna('')
        enhanced_data['ROE'] = enhanced_data['ROE'].fillna(np.nan)
        enhanced_data['ROCE'] = enhanced_data['ROCE'].fillna(np.nan)
        
        logger.info(f"Enhanced data shape: {enhanced_data.shape}")
        
        # Log enhancement summary
        market_cap_count = enhanced_data['Market_Cap'].notna().sum()
        sector_count = (enhanced_data['Sector'] != '').sum()
        industry_count = (enhanced_data['Industry'] != '').sum()
        roe_count = enhanced_data['ROE'].notna().sum()
        roce_count = enhanced_data['ROCE'].notna().sum()
        
        logger.info(f"Enhancement summary:")
        logger.info(f"  Market Cap data: {market_cap_count}/{len(enhanced_data)} stocks")
        logger.info(f"  Sector data: {sector_count}/{len(enhanced_data)} stocks")
        logger.info(f"  Industry data: {industry_count}/{len(enhanced_data)} stocks")
        logger.info(f"  ROE data: {roe_count}/{len(enhanced_data)} stocks")
        logger.info(f"  ROCE data: {roce_count}/{len(enhanced_data)} stocks")
        
        return enhanced_data
    
    def upload_enhanced_data(self, enhanced_data: pd.DataFrame) -> str:
        """Upload enhanced data to S3"""
        try:
            # Generate filename with current date
            current_date = datetime.now().strftime('%Y-%m-%d')
            enhanced_key = f"{self.prefix}/adjusted-eq-data-enhanced-{current_date}.csv"
            
            logger.info(f"Uploading enhanced data to: s3://{self.bucket_name}/{enhanced_key}")
            
            # Convert DataFrame to CSV string and upload
            csv_data = enhanced_data.to_csv(index=False)
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=enhanced_key,
                Body=csv_data,
                ContentType='text/csv'
            )
            
            logger.info(f"Enhanced data uploaded successfully!")
            return enhanced_key
            
        except Exception as e:
            logger.error(f"Upload failed: {str(e)}")
            raise
    
    def run_complete_enhancement(self, batch_size: int = 50, delay_between_batches: int = 300) -> str:
        """Run the complete enhancement process"""
        try:
            logger.info("Starting Screener data enhancement process...")
            
            # Step 1: Download latest returns data
            self.download_returns_data()
            
            # Step 2: Extract Screener data for all stocks
            self.extract_screener_data_for_all_stocks(batch_size, delay_between_batches)
            
            # Step 3: Merge and enhance data
            enhanced_data = self.merge_and_enhance_data()
            
            # Step 4: Upload enhanced data
            enhanced_key = self.upload_enhanced_data(enhanced_data)
            
            logger.info("Enhancement process completed successfully!")
            return enhanced_key
            
        except Exception as e:
            logger.error(f"Enhancement process failed: {str(e)}")
            raise
    
    def display_summary(self):
        """Display a summary of the enhanced data"""
        if self.returns_data is None:
            logger.info("No data to display.")
            return
        
        print("\n" + "="*60)
        print("SCREENER DATA ENHANCEMENT SUMMARY")
        print("="*60)
        
        print(f"Total stocks processed: {len(self.returns_data)}")
        
        if self.screener_data is not None:
            market_cap_data = self.screener_data['Market_Cap'].dropna()
            sector_data = self.screener_data['Sector'].dropna()
            industry_data = self.screener_data['Industry'].dropna()
            roe_data = self.screener_data['ROE'].dropna()
            roce_data = self.screener_data['ROCE'].dropna()
            
            print(f"\nScreener Data Extraction Results:")
            print(f"  Market Cap data: {len(market_cap_data)} stocks")
            print(f"  Sector data: {len(sector_data)} stocks")
            print(f"  Industry data: {len(industry_data)} stocks")
            print(f"  ROE data: {len(roe_data)} stocks")
            print(f"  ROCE data: {len(roce_data)} stocks")
            
            if len(market_cap_data) > 0:
                print(f"  Market Cap range: {market_cap_data.min():.2f} Cr to {market_cap_data.max():.2f} Cr")
            if len(roe_data) > 0:
                print(f"  ROE range: {roe_data.min():.2f}% to {roe_data.max():.2f}%")
            if len(roce_data) > 0:
                print(f"  ROCE range: {roce_data.min():.2f}% to {roce_data.max():.2f}%")
            
            # Show top sectors
            if len(sector_data) > 0:
                top_sectors = sector_data.value_counts().head(5)
                print(f"  Top 5 Sectors:")
                for sector, count in top_sectors.items():
                    print(f"    {sector}: {count} stocks")


def main():
    """Main function to run the enhancement process"""
    try:
        # Configuration
        BUCKET_NAME = "trading-platform-csvs"
        PREFIX = "adjusted-eq-data"
        
        # AWS Credentials (from environment variables)
        credentials = {
            'access_key': os.getenv('AWS_ACCESS_KEY_ID'),
            'secret_key': os.getenv('AWS_SECRET_ACCESS_KEY')
        }
        
        # Create enhancer and run process
        enhancer = ScreenerDataEnhancer(
            bucket_name=BUCKET_NAME,
            prefix=PREFIX,
            credentials=credentials
        )
        
        # Run complete enhancement process
        enhanced_key = enhancer.run_complete_enhancement(
            batch_size=50,           # Process 50 symbols per batch
            delay_between_batches=300  # 5 minutes delay between batches
        )
        
        # Display summary
        enhancer.display_summary()
        
        print(f"\nSUCCESS! Screener data enhancement completed!")
        print(f"Enhanced file: s3://{BUCKET_NAME}/{enhanced_key}")
        print(f"Features added:")
        print(f"  - Market Cap for all stocks")
        print(f"  - Sector classification")
        print(f"  - Industry classification")
        print(f"  - ROE (Return on Equity)")
        print(f"  - ROCE (Return on Capital Employed)")
        
    except Exception as e:
        logger.error(f"Enhancement process failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()
