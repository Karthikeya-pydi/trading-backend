import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class ReturnsCalculator:
    def __init__(self, csv_file_path):
        """
        Initialize the ReturnsCalculator with the CSV file path
        
        Args:
            csv_file_path (str): Path to the CSV file containing stock data
        """
        self.csv_file_path = csv_file_path
        self.data = None
        self.returns_data = None
        
    def load_data(self, target_date=None):
        """Load and preprocess the CSV data, filtering for stocks available on target date"""
        if target_date is None:
            target_date = datetime.now().strftime('%Y-%m-%d')
            
        print(f"Loading CSV data for stocks available on {target_date}...")
        
        # Check if data is already loaded (for S3 usage)
        if self.data is not None:
            print("Data already loaded, skipping file read...")
        else:
            # Read the CSV file
            self.data = pd.read_csv(self.csv_file_path)
        
        # Convert Date column to datetime
        self.data['Date'] = pd.to_datetime(self.data['Date'])
        
        # Filter only active stocks
        self.data = self.data[self.data['Status'] == 'Active']
        
        # Convert target_date to datetime
        target_date_dt = pd.to_datetime(target_date)
        
        # Get list of symbols that have data on the target date
        symbols_on_target_date = self.data[self.data['Date'] == target_date_dt]['Symbol'].unique()
        print(f"Found {len(symbols_on_target_date)} symbols with data on {target_date}")
        
        # Filter data to only include symbols that have data on target date
        self.data = self.data[self.data['Symbol'].isin(symbols_on_target_date)]
        
        # Sort by Symbol and Date
        self.data = self.data.sort_values(['Symbol', 'Date'])
        
        # Convert Close price to numeric, handling any non-numeric values
        self.data['Close'] = pd.to_numeric(self.data['Close'], errors='coerce')
        
        # Remove rows with missing Close prices
        self.data = self.data.dropna(subset=['Close'])
        
        print(f"Loaded {len(self.data)} records for {self.data['Symbol'].nunique()} unique symbols available on {target_date}")
        
    def calculate_returns(self, symbol_data, periods):
        """
        Calculate returns for different time periods for a given symbol
        
        Args:
            symbol_data (DataFrame): Data for a specific symbol
            periods (dict): Dictionary of period names and their days
            
        Returns:
            dict: Returns for different periods
        """
        if len(symbol_data) < 2:
            return {period: np.nan for period in periods.keys()}
        
        # Get the latest date and price
        latest_date = symbol_data['Date'].max()
        latest_price = symbol_data[symbol_data['Date'] == latest_date]['Close'].iloc[0]
        
        returns = {}
        
        for period_name, days in periods.items():
            # Calculate the target date
            target_date = latest_date - timedelta(days=days)
            
            # Find the closest date in the data (within 5 days tolerance)
            available_dates = symbol_data['Date'].sort_values()
            target_data = available_dates[available_dates <= target_date]
            
            if len(target_data) > 0:
                # Get the closest date
                closest_date = target_data.iloc[-1]
                historical_price = symbol_data[symbol_data['Date'] == closest_date]['Close'].iloc[0]
                
                # Calculate return
                if historical_price > 0:
                    returns[period_name] = ((latest_price - historical_price) / historical_price) * 100
                else:
                    returns[period_name] = np.nan
            else:
                returns[period_name] = np.nan
                
        return returns
    
    def calculate_turnover(self, symbol_data):
        """
        Calculate turnover as: (Last 6 months close price average) × Current volume
        
        Args:
            symbol_data (DataFrame): Data for a specific symbol
            
        Returns:
            float: Turnover value
        """
        # Calculate 6 months as approximately 180 days
        six_months_days = 180
        
        if len(symbol_data) < six_months_days:
            return np.nan
        
        # Get the latest date and current volume
        latest_date = symbol_data['Date'].max()
        current_volume = symbol_data[symbol_data['Date'] == latest_date]['Volume'].iloc[0]
        
        # Get last 6 months of data (excluding the current date)
        historical_data = symbol_data[symbol_data['Date'] < latest_date].tail(six_months_days)
        
        if len(historical_data) < six_months_days:
            return np.nan
        
        # Calculate average close price of last 6 months
        avg_close_price = historical_data['Close'].mean()
        
        # Calculate turnover
        turnover = avg_close_price * current_volume
        
        return turnover
    
    def process_all_symbols(self):
        """Process all symbols and calculate returns"""
        print("Calculating returns for all symbols...")
        
        # Define time periods in days
        periods = {
            '1_Week': 7,
            '1_Month': 30,
            '3_Months': 90,
            '6_Months': 180,
            '9_Months': 270,
            '1_Year': 365,
            '3_Years': 1095,
            '5_Years': 1825
        }
        
        results = []
        
        # Group by symbol
        for symbol, group in self.data.groupby('Symbol'):
            if len(group) > 1:  # Need at least 2 data points
                returns = self.calculate_returns(group, periods)
                turnover = self.calculate_turnover(group)
                
                # Get additional information
                latest_data = group[group['Date'] == group['Date'].max()].iloc[0]
                
                result = {
                    'Symbol': symbol,
                    'Fincode': latest_data['Fincode'],
                    'ISIN': latest_data['ISIN'],
                    'Latest_Date': latest_data['Date'],
                    'Latest_Close': latest_data['Close'],
                    'Latest_Volume': latest_data['Volume'],
                    'Turnover': turnover
                }
                
                # Add returns
                result.update(returns)
                results.append(result)
        
        self.returns_data = pd.DataFrame(results)
        print(f"Calculated returns for {len(self.returns_data)} symbols")
        
    def save_results(self, output_file='stock_returns.csv'):
        """Save the results to a CSV file"""
        if self.returns_data is not None:
            self.returns_data.to_csv(output_file, index=False)
            print(f"Results saved to {output_file}")
        else:
            print("No results to save. Run process_all_symbols() first.")
    
    def calculate_stock_scores(self, normalization_method='percentile'):
        """
        Calculate raw score and normalized score for stocks using weighted formula:
        (1M × -10% + 3M × 25% + 6M × 25% + 9M × 40% + 1Y × 20%)
        
        Weights:
        - 1M = -10%
        - 3M = 25%
        - 6M = 25%
        - 9M = 40%
        - 1Y = 20%
        
        Args:
            normalization_method (str): 'percentile', 'zscore', or 'minmax'
        """
        if self.returns_data is None:
            print("No returns data available. Run process_all_symbols() first.")
            return
        
        print("Calculating stock scores...")
        
        # Define weights
        weights = {
            '1_Month': -0.10,    # -10%
            '3_Months': 0.25,     # 25%
            '6_Months': 0.25,     # 25%
            '9_Months': 0.40,     # 40%
            '1_Year': 0.20        # 20%
        }
        
        # Calculate raw score for each stock
        raw_scores = []
        
        for _, row in self.returns_data.iterrows():
            # Check if all required return data is available
            required_columns = ['1_Month', '3_Months', '6_Months', '9_Months', '1_Year']
            if all(pd.notna(row[col]) for col in required_columns):
                # Calculate weighted score
                raw_score = (
                    row['1_Month'] * weights['1_Month'] +
                    row['3_Months'] * weights['3_Months'] +
                    row['6_Months'] * weights['6_Months'] +
                    row['9_Months'] * weights['9_Months'] +
                    row['1_Year'] * weights['1_Year']
                ) * 100
                
                raw_scores.append(raw_score)
            else:
                raw_scores.append(np.nan)
        
        # Add raw scores to the dataframe
        self.returns_data['Raw_Score'] = raw_scores
        
        # Calculate normalized scores using different methods
        valid_scores = self.returns_data['Raw_Score'].dropna()
        if len(valid_scores) > 0:
            if normalization_method == 'percentile':
                # Use percentile-based normalization to handle outliers better
                p1 = valid_scores.quantile(0.01)  # 1st percentile
                p99 = valid_scores.quantile(0.99)  # 99th percentile
                
                if p99 != p1:
                    normalized_scores = ((self.returns_data['Raw_Score'] - p1) / 
                                       (p99 - p1)) * 100
                    normalized_scores = np.clip(normalized_scores, 0, 100)
                else:
                    normalized_scores = pd.Series([50.0] * len(self.returns_data), 
                                                index=self.returns_data.index)
                method_desc = "percentile-based normalization (1st-99th percentile range)"
                
            elif normalization_method == 'zscore':
                # Use Z-score normalization
                mean_score = valid_scores.mean()
                std_score = valid_scores.std()
                
                if std_score > 0:
                    z_scores = (self.returns_data['Raw_Score'] - mean_score) / std_score
                    # Convert Z-scores to 0-100 scale (assuming normal distribution)
                    normalized_scores = 50 + (z_scores * 15)  # 15 is roughly 3 standard deviations
                    normalized_scores = np.clip(normalized_scores, 0, 100)
                else:
                    normalized_scores = pd.Series([50.0] * len(self.returns_data), 
                                                index=self.returns_data.index)
                method_desc = "Z-score normalization"
                
            elif normalization_method == 'minmax':
                # Traditional min-max normalization
                min_score = valid_scores.min()
                max_score = valid_scores.max()
                
                if max_score != min_score:
                    normalized_scores = ((self.returns_data['Raw_Score'] - min_score) / 
                                       (max_score - min_score)) * 100
                else:
                    normalized_scores = pd.Series([50.0] * len(self.returns_data), 
                                                index=self.returns_data.index)
                method_desc = "min-max normalization"
            
            else:
                raise ValueError("normalization_method must be 'percentile', 'zscore', or 'minmax'")
            
            self.returns_data['Normalized_Score'] = normalized_scores
        else:
            self.returns_data['Normalized_Score'] = np.nan
            method_desc = "no valid scores"
        
        print(f"Calculated scores for {len(valid_scores)} stocks with complete data")
        print(f"Using {method_desc}")
        
    def display_scoring_summary(self):
        """Display a summary of the scoring results"""
        if self.returns_data is None or 'Raw_Score' not in self.returns_data.columns:
            print("No scoring data available. Run calculate_stock_scores() first.")
            return
        
        print("\n" + "="*80)
        print("STOCK SCORING SUMMARY")
        print("="*80)
        
        # Display basic statistics
        valid_scores = self.returns_data['Raw_Score'].dropna()
        print(f"Total stocks with scores: {len(valid_scores)}")
        
        if len(valid_scores) > 0:
            print(f"\nRaw Score Statistics:")
            print("-" * 50)
            print(f"Mean:    {valid_scores.mean():8.2f}")
            print(f"Median:  {valid_scores.median():8.2f}")
            print(f"Min:     {valid_scores.min():8.2f}")
            print(f"Max:     {valid_scores.max():8.2f}")
            print(f"Std Dev: {valid_scores.std():8.2f}")
            
            print(f"\nNormalized Score Statistics:")
            print("-" * 50)
            valid_norm_scores = self.returns_data['Normalized_Score'].dropna()
            print(f"Mean:    {valid_norm_scores.mean():8.2f}")
            print(f"Median:  {valid_norm_scores.median():8.2f}")
            print(f"Min:     {valid_norm_scores.min():8.2f}")
            print(f"Max:     {valid_norm_scores.max():8.2f}")
            print(f"Std Dev: {valid_norm_scores.std():8.2f}")
            
            # Show distribution ranges
            print(f"\nNormalized Score Distribution:")
            print("-" * 50)
            ranges = [(0, 20), (20, 40), (40, 60), (60, 80), (80, 100)]
            for low, high in ranges:
                count = len(valid_norm_scores[(valid_norm_scores >= low) & (valid_norm_scores < high)])
                percentage = (count / len(valid_norm_scores)) * 100
                print(f"{low:2d}-{high:2d}: {count:4d} stocks ({percentage:5.1f}%)")
            
            # Display top 10 stocks by raw score
            print("\nTop 10 Stocks by Raw Score:")
            print("-" * 80)
            print(f"{'Rank':<4} {'Symbol':<15} {'Raw Score':<10} {'Norm Score':<12} {'1M':<8} {'3M':<8} {'6M':<8} {'9M':<8} {'1Y':<8}")
            print("-" * 80)
            
            top_stocks = self.returns_data.nlargest(10, 'Raw_Score')
            for i, (_, row) in enumerate(top_stocks.iterrows(), 1):
                print(f"{i:<4} {row['Symbol']:<15} {row['Raw_Score']:<10.2f} {row['Normalized_Score']:<12.2f} "
                      f"{row['1_Month']:<8.2f} {row['3_Months']:<8.2f} {row['6_Months']:<8.2f} {row['9_Months']:<8.2f} {row['1_Year']:<8.2f}")
            
            # Display bottom 10 stocks by raw score
            print("\nBottom 10 Stocks by Raw Score:")
            print("-" * 80)
            print(f"{'Rank':<4} {'Symbol':<15} {'Raw Score':<10} {'Norm Score':<12} {'1M':<8} {'3M':<8} {'6M':<8} {'9M':<8} {'1Y':<8}")
            print("-" * 80)
            
            bottom_stocks = self.returns_data.nsmallest(10, 'Raw_Score')
            for i, (_, row) in enumerate(bottom_stocks.iterrows(), 1):
                print(f"{i:<4} {row['Symbol']:<15} {row['Raw_Score']:<10.2f} {row['Normalized_Score']:<12.2f} "
                      f"{row['1_Month']:<8.2f} {row['3_Months']:<8.2f} {row['6_Months']:<8.2f} {row['9_Months']:<8.2f} {row['1_Year']:<8.2f}")
    
    def display_summary(self):
        """Display a summary of the results"""
        if self.returns_data is None:
            print("No results to display. Run process_all_symbols() first.")
            return
        
        print("\n" + "="*80)
        print("STOCK RETURNS SUMMARY")
        print("="*80)
        
        # Display basic statistics
        print(f"Total symbols processed: {len(self.returns_data)}")
        print(f"Latest data date: {self.returns_data['Latest_Date'].max()}")
        
        # Display returns statistics for each period
        return_columns = ['1_Week', '1_Month', '3_Months', '6_Months', '9_Months', '1_Year', '3_Years', '5_Years']
        
        # Display turnover statistics
        if 'Turnover' in self.returns_data.columns:
            valid_turnover = self.returns_data['Turnover'].dropna()
            if len(valid_turnover) > 0:
                print(f"\nTurnover Statistics:")
                print("-" * 50)
                print(f"Mean:    {valid_turnover.mean():15,.2f}")
                print(f"Median:  {valid_turnover.median():15,.2f}")
                print(f"Min:     {valid_turnover.min():15,.2f}")
                print(f"Max:     {valid_turnover.max():15,.2f}")
                print(f"Count:   {len(valid_turnover):15d}")
        
        print("\nReturns Statistics:")
        print("-" * 50)
        for col in return_columns:
            if col in self.returns_data.columns:
                valid_returns = self.returns_data[col].dropna()
                if len(valid_returns) > 0:
                    print(f"{col:12}: Mean: {valid_returns.mean():8.2f}%, "
                          f"Median: {valid_returns.median():8.2f}%, "
                          f"Count: {len(valid_returns):4d}")
        
        # Display top performers for each period
        print("\nTop 5 Performers (1 Year):")
        print("-" * 50)
        if '1_Year' in self.returns_data.columns:
            top_performers = self.returns_data.nlargest(5, '1_Year')[['Symbol', '1_Year', 'Latest_Close']]
            for _, row in top_performers.iterrows():
                print(f"{row['Symbol']:15} {row['1_Year']:8.2f}% ₹{row['Latest_Close']:8.2f}")
        
        # Display worst performers for each period
        print("\nBottom 5 Performers (1 Year):")
        print("-" * 50)
        if '1_Year' in self.returns_data.columns:
            bottom_performers = self.returns_data.nsmallest(5, '1_Year')[['Symbol', '1_Year', 'Latest_Close']]
            for _, row in bottom_performers.iterrows():
                print(f"{row['Symbol']:15} {row['1_Year']:8.2f}% ₹{row['Latest_Close']:8.2f}")
    
    def run_analysis(self, output_file=None, target_date=None):
        """Run the complete analysis"""
        if target_date is None:
            target_date = datetime.now().strftime('%Y-%m-%d')
        if output_file is None:
            output_file = f'stock_returns_{target_date}.csv'
            
        print("Starting Stock Returns Analysis...")
        print("="*50)
        
        try:
            # Load data
            self.load_data(target_date)
            
            # Process all symbols
            self.process_all_symbols()
            
            # Save results
            self.save_results(output_file)
            
            # Display summary
            self.display_summary()
            
            print("\nAnalysis completed successfully!")
            
        except Exception as e:
            print(f"Error during analysis: {str(e)}")
            raise
    
    def run_analysis_with_scoring(self, output_file=None, target_date=None):
        """Run the complete analysis including scoring"""
        if target_date is None:
            target_date = datetime.now().strftime('%Y-%m-%d')
        if output_file is None:
            output_file = f'stock_returns_{target_date}.csv'
            
        print("Starting Stock Returns Analysis with Scoring...")
        print("="*60)
        
        try:
            # Load data
            self.load_data(target_date)
            
            # Process all symbols
            self.process_all_symbols()
            
            # Calculate stock scores
            self.calculate_stock_scores()
            
            # Save results with scores
            self.save_results(output_file)
            
            # Display summary
            self.display_summary()
            
            # Display scoring summary
            self.display_scoring_summary()
            
            print("\nAnalysis with scoring completed successfully!")
            
        except Exception as e:
            print(f"Error during analysis: {str(e)}")
            raise

def main():
    """Main function to run the analysis"""
    # Get current date
    current_date = datetime.now().strftime('%Y-%m-%d')
    
    # File path - try current date first, then fallback to most recent
    csv_file = f"adjusted-eq-data-{current_date}.csv"
    
    if not os.path.exists(csv_file):
        # Look for the most recent CSV file
        csv_files = list(Path('.').glob('adjusted-eq-data-*.csv'))
        if csv_files:
            csv_file = max(csv_files, key=os.path.getmtime)
            print(f"Using most recent CSV file: {csv_file}")
        else:
            print("No CSV files found!")
            return
    
    # Create calculator instance
    calculator = ReturnsCalculator(str(csv_file))
    
    # Run analysis with scoring
    calculator.run_analysis_with_scoring()
    
    # You can also access the results programmatically
    # print(calculator.returns_data.head())

def run_scoring_only():
    """Run scoring on existing stock_returns_2025-09-09.csv data"""
    # Load existing returns data
    calculator = ReturnsCalculator("adjusted-eq-data-2025-09-09.csv")
    calculator.returns_data = pd.read_csv("stock_returns_2025-09-09.csv")
    
    # Calculate scores
    calculator.calculate_stock_scores()
    
    # Display scoring summary
    calculator.display_scoring_summary()
    
    # Save results with scores
    calculator.save_results("stock_returns_2025-09-09.csv")

if __name__ == "__main__":
    main()
