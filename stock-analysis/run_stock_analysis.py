"""
Stock Analysis Pipeline Runner

This script runs the comprehensive stock analysis pipeline on the H5 data file
and saves the results to CSV format. It processes each stock individually
from 2003 onwards, creating metrics and flags unique to each stock's performance.
"""

import pandas as pd
import numpy as np
from datetime import datetime
import os
import sys
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Add the current directory to the path
sys.path.append(os.path.dirname(__file__))

from stock_analysis_service import StockAnalysisService
from loguru import logger

def setup_logging():
    """Setup logging configuration."""
    logger.remove()  # Remove default handler
    logger.add(
        "stock_analysis.log",
        rotation="10 MB",
        retention="7 days",
        level="INFO",
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
    )
    logger.add(
        sys.stdout,
        level="INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}"
    )

def main():
    """Main function to run the stock analysis pipeline."""
    setup_logging()
    
    logger.info("=" * 60)
    logger.info("STOCK ANALYSIS PIPELINE - STARTING")
    logger.info("=" * 60)
    
    try:
        # Initialize the analysis service
        analysis_service = StockAnalysisService("../Our_Nseadjprice.h5")
        
        # Load data
        logger.info("Loading data from H5 file...")
        data = analysis_service.load_data()
        logger.info(f"Data loaded successfully: {len(data)} records")
        
        # Get unique stocks
        unique_stocks = analysis_service.get_unique_stocks()
        logger.info(f"Found {len(unique_stocks)} unique stocks")
        
        # Ask user for analysis scope
        print("\n" + "=" * 60)
        print("ANALYSIS OPTIONS")
        print("=" * 60)
        print("1. Analyze all stocks (this may take several hours)")
        print("2. Analyze first 10 stocks (for testing)")
        print("3. Analyze first 50 stocks")
        print("4. Analyze first 100 stocks")
        print("5. Analyze specific stocks (enter symbols separated by commas)")
        
        choice = input("\nEnter your choice (1-5): ").strip()
        
        if choice == "1":
            max_stocks = None
            logger.info("Analyzing ALL stocks")
        elif choice == "2":
            max_stocks = 10
            logger.info("Analyzing first 10 stocks")
        elif choice == "3":
            max_stocks = 50
            logger.info("Analyzing first 50 stocks")
        elif choice == "4":
            max_stocks = 100
            logger.info("Analyzing first 100 stocks")
        elif choice == "5":
            symbols_input = input("Enter stock symbols separated by commas: ").strip()
            symbols = [s.strip().upper() for s in symbols_input.split(',')]
            # Filter to only include symbols that exist in the data
            valid_symbols = [s for s in symbols if s in unique_stocks]
            if not valid_symbols:
                logger.error("No valid symbols found")
                return
            logger.info(f"Analyzing specific stocks: {valid_symbols}")
        else:
            logger.error("Invalid choice")
            return
        
        # Run analysis
        start_time = datetime.now()
        
        if choice == "5":
            # Analyze specific stocks
            results = {}
            for symbol in valid_symbols:
                logger.info(f"Analyzing stock: {symbol}")
                result = analysis_service.analyze_single_stock(symbol)
                results[symbol] = result
            
            analysis_results = {
                'summary': {
                    'total_stocks': len(valid_symbols),
                    'successful_analyses': len([r for r in results.values() if 'error' not in r]),
                    'failed_analyses': len([r for r in results.values() if 'error' in r]),
                    'analysis_timestamp': start_time.isoformat()
                },
                'results': results
            }
        else:
            # Analyze with max_stocks limit
            analysis_results = analysis_service.analyze_all_stocks(max_stocks)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Print summary
        summary = analysis_results['summary']
        logger.info("=" * 60)
        logger.info("ANALYSIS COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total stocks processed: {summary['total_stocks']}")
        logger.info(f"Successful analyses: {summary['successful_analyses']}")
        logger.info(f"Failed analyses: {summary['failed_analyses']}")
        logger.info(f"Duration: {duration}")
        logger.info(f"Average time per stock: {duration.total_seconds() / summary['total_stocks']:.2f} seconds")
        
        # Export results
        logger.info("Exporting results to CSV...")
        
        # Create output directory
        output_dir = Path("analysis_results")
        output_dir.mkdir(exist_ok=True)
        
        # Export detailed results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        detailed_output_path = output_dir / f"stock_analysis_detailed_{timestamp}.csv"
        
        try:
            exported_path = analysis_service.export_results_to_csv(
                analysis_results, 
                str(detailed_output_path)
            )
            if exported_path:
                logger.info(f"Detailed results exported to: {exported_path}")
            else:
                logger.warning("No data to export")
        except Exception as e:
            logger.error(f"Error exporting detailed results: {e}")
        
        # Export summary statistics
        summary_data = []
        for symbol, result in analysis_results['results'].items():
            if 'error' not in result:
                desc_stats = result.get('descriptive_stats', {})
                global_analysis = result.get('global_analysis', {})
                per_stock_analysis = result.get('per_stock_analysis', {})
                
                summary_data.append({
                    'symbol': symbol,
                    'n_days': desc_stats.get('n_days', 0),
                    'pct_missing': desc_stats.get('pct_missing', 100.0),
                    'start_date': desc_stats.get('start_date'),
                    'end_date': desc_stats.get('end_date'),
                    'mean_return': desc_stats.get('mean_return', np.nan),
                    'std_return': desc_stats.get('std_return', np.nan),
                    'skew_return': desc_stats.get('skew_return', np.nan),
                    'kurtosis_return': desc_stats.get('kurtosis_return', np.nan),
                    'min_return': desc_stats.get('min_return', np.nan),
                    'max_return': desc_stats.get('max_return', np.nan),
                    'illiquid_flag': desc_stats.get('illiquid_flag', True),
                    'global_median': global_analysis.get('global_median', np.nan),
                    'global_mad': global_analysis.get('global_mad', np.nan),
                    'per_stock_median': per_stock_analysis.get('per_stock_median', np.nan),
                    'per_stock_mad': per_stock_analysis.get('per_stock_mad', np.nan),
                    'total_global_outliers': result.get('enhanced_data', pd.DataFrame()).get('global_outlier_flag', pd.Series()).sum() if 'enhanced_data' in result else 0,
                    'total_robust_outliers': result.get('enhanced_data', pd.DataFrame()).get('robust_outlier_flag', pd.Series()).sum() if 'enhanced_data' in result else 0,
                    'total_very_extreme': result.get('enhanced_data', pd.DataFrame()).get('very_extreme_flag', pd.Series()).sum() if 'enhanced_data' in result else 0,
                    'total_mild_anomalies': result.get('enhanced_data', pd.DataFrame()).get('mild_anomaly_flag', pd.Series()).sum() if 'enhanced_data' in result else 0,
                    'total_major_anomalies': result.get('enhanced_data', pd.DataFrame()).get('major_anomaly_flag', pd.Series()).sum() if 'enhanced_data' in result else 0
                })
            else:
                summary_data.append({
                    'symbol': symbol,
                    'error': result['error'],
                    'n_days': 0,
                    'pct_missing': 100.0,
                    'illiquid_flag': True
                })
        
        # Save summary
        summary_df = pd.DataFrame(summary_data)
        summary_output_path = output_dir / f"stock_analysis_summary_{timestamp}.csv"
        summary_df.to_csv(summary_output_path, index=False)
        logger.info(f"Summary results exported to: {summary_output_path}")
        
        # Print sample of results
        logger.info("\n" + "=" * 60)
        logger.info("SAMPLE RESULTS (First 5 stocks)")
        logger.info("=" * 60)
        print(summary_df.head().to_string(index=False))
        
        logger.info("\n" + "=" * 60)
        logger.info("ANALYSIS PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 60)
        logger.info(f"Results saved in: {output_dir}")
        logger.info(f"Log file: stock_analysis.log")
        
    except Exception as e:
        logger.error(f"Error in main analysis pipeline: {e}")
        raise

if __name__ == "__main__":
    main()
