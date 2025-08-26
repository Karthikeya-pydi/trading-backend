import pandas as pd
import numpy as np
from datetime import datetime, timedelta
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
        
    def load_data(self):
        """Load and preprocess the CSV data"""
        print("Loading CSV data...")
        
        # Read the CSV file
        self.data = pd.read_csv(self.csv_file_path)
        
        # Convert Date column to datetime
        self.data['Date'] = pd.to_datetime(self.data['Date'])
        
        # Filter only active stocks
        self.data = self.data[self.data['Status'] == 'Active']
        
        # Sort by Symbol and Date
        self.data = self.data.sort_values(['Symbol', 'Date'])
        
        # Convert Close price to numeric, handling any non-numeric values
        self.data['Close'] = pd.to_numeric(self.data['Close'], errors='coerce')
        
        # Remove rows with missing Close prices
        self.data = self.data.dropna(subset=['Close'])
        
        print(f"Loaded {len(self.data)} records for {self.data['Symbol'].nunique()} unique symbols")
        
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
    
    def process_all_symbols(self):
        """Process all symbols and calculate returns"""
        print("Calculating returns for all symbols...")
        
        # Define time periods in days
        periods = {
            '1_Week': 7,
            '1_Month': 30,
            '3_Months': 90,
            '6_Months': 180,
            '1_Year': 365,
            '3_Years': 1095,
            '5_Years': 1825
        }
        
        results = []
        
        # Group by symbol
        for symbol, group in self.data.groupby('Symbol'):
            if len(group) > 1:  # Need at least 2 data points
                returns = self.calculate_returns(group, periods)
                
                # Get additional information
                latest_data = group[group['Date'] == group['Date'].max()].iloc[0]
                
                result = {
                    'Symbol': symbol,
                    'Fincode': latest_data['Fincode'],
                    'ISIN': latest_data['ISIN'],
                    'Latest_Date': latest_data['Date'],
                    'Latest_Close': latest_data['Close'],
                    'Latest_Volume': latest_data['Volume']
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
        return_columns = ['1_Week', '1_Month', '3_Months', '6_Months', '1_Year', '3_Years', '5_Years']
        
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
    
    def run_analysis(self, output_file='stock_returns.csv'):
        """Run the complete analysis"""
        print("Starting Stock Returns Analysis...")
        print("="*50)
        
        try:
            # Load data
            self.load_data()
            
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

def main():
    """Main function to run the analysis"""
    # File path
    csv_file = "adjusted-eq-data-2025-08-25.csv"
    
    # Create calculator instance
    calculator = ReturnsCalculator(csv_file)
    
    # Run analysis
    calculator.run_analysis()
    
    # You can also access the results programmatically
    # print(calculator.returns_data.head())

if __name__ == "__main__":
    main()
