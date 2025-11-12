"""
Data Formatter for LLM Service

Formats portfolio, returns, and bhavcopy data into readable strings for LLM context.
"""

from typing import Dict, List, Optional
from loguru import logger


class DataFormatter:
    """Format trading data for LLM context"""
    
    @staticmethod
    def format_portfolio_for_llm(holdings_data: Dict) -> str:
        """
        Format portfolio/holdings data for LLM
        
        Args:
            holdings_data: Dictionary containing holdings data from IIFL service
            
        Returns:
            Formatted string for LLM context
        """
        try:
            if not holdings_data:
                logger.warning("Portfolio data is None or empty")
                return "Portfolio data not available."
            
            # Check if response has "type" field (IIFL API response format)
            if holdings_data.get("type") != "success":
                error_msg = holdings_data.get("description", "Unknown error")
                logger.warning(f"Portfolio data fetch failed: {error_msg}")
                return f"Portfolio data not available. Error: {error_msg}"
            
            # Extract holdings from nested structure
            # IIFL API returns: {"type": "success", "result": {"RMSHoldings": {"Holdings": {...}}}}
            result = holdings_data.get("result", {})
            
            # Try different possible structures
            holdings = None
            if isinstance(result, dict):
                # Check for nested RMSHoldings structure
                rms_holdings = result.get("RMSHoldings", {})
                if isinstance(rms_holdings, dict):
                    holdings_dict = rms_holdings.get("Holdings", {})
                    if isinstance(holdings_dict, dict):
                        # Holdings is a dictionary of holdings
                        holdings = list(holdings_dict.values())
                    elif isinstance(holdings_dict, list):
                        holdings = holdings_dict
                
                # If not found, check if result is directly a list
                if not holdings and isinstance(result, list):
                    holdings = result
            
            # If still not found, check if result has a list directly
            if not holdings:
                # Try to find any list in the result
                if isinstance(result, dict):
                    for key, value in result.items():
                        if isinstance(value, dict):
                            holdings_dict = value.get("Holdings", {})
                            if isinstance(holdings_dict, dict):
                                holdings = list(holdings_dict.values())
                                break
                            elif isinstance(holdings_dict, list):
                                holdings = holdings_dict
                                break
                        elif isinstance(value, list):
                            holdings = value
                            break
            
            if not holdings or len(holdings) == 0:
                logger.warning("No holdings found in portfolio data")
                return "No holdings found in portfolio."
            
            # Limit to top 20 holdings to avoid token overflow
            top_holdings = holdings[:20] if isinstance(holdings, list) else []
            
            formatted_lines = ["=== PORTFOLIO HOLDINGS ==="]
            
            total_investment = 0
            total_current_value = 0
            processed_count = 0
            
            for holding in top_holdings:
                if not isinstance(holding, dict):
                    continue
                
                # Try different field names for stock symbol
                stock_name = (
                    holding.get("TradingSymbol") or 
                    holding.get("Symbol") or 
                    holding.get("InstrumentName") or 
                    holding.get("stock_name") or
                    "N/A"
                )
                
                # Try different field names for quantity
                quantity = (
                    holding.get("Quantity") or 
                    holding.get("quantity") or 
                    holding.get("HoldingQuantity") or
                    0
                )
                
                # Try different field names for price
                avg_price = (
                    holding.get("Price") or 
                    holding.get("AveragePrice") or 
                    holding.get("avg_price") or
                    holding.get("PurchasePrice") or
                    0
                )
                
                # Try different field names for last traded price
                ltp = (
                    holding.get("LastTradedPrice") or 
                    holding.get("LTP") or 
                    holding.get("current_price") or 
                    holding.get("CurrentPrice") or
                    avg_price
                )
                
                # Convert to float/int
                try:
                    quantity = float(quantity) if quantity else 0
                    avg_price = float(avg_price) if avg_price else 0
                    ltp = float(ltp) if ltp else avg_price
                except (ValueError, TypeError):
                    continue
                
                if quantity > 0 and avg_price > 0:
                    invested_value = quantity * avg_price
                    current_value = quantity * ltp
                    pnl = current_value - invested_value
                    pnl_percent = (pnl / invested_value * 100) if invested_value > 0 else 0
                    
                    total_investment += invested_value
                    total_current_value += current_value
                    processed_count += 1
                    
                    formatted_lines.append(
                        f"• {stock_name}: Qty={quantity:.0f}, "
                        f"Avg Price=₹{avg_price:.2f}, LTP=₹{ltp:.2f}, "
                        f"Invested=₹{invested_value:.2f}, Current=₹{current_value:.2f}, "
                        f"P&L=₹{pnl:.2f} ({pnl_percent:+.2f}%)"
                    )
            
            if processed_count == 0:
                logger.warning("No valid holdings found after processing")
                return "No valid holdings found in portfolio."
            
            if len(holdings) > 20:
                formatted_lines.append(f"\n... and {len(holdings) - 20} more holdings")
            
            # Add summary
            total_pnl = total_current_value - total_investment
            total_pnl_percent = (total_pnl / total_investment * 100) if total_investment > 0 else 0
            
            formatted_lines.append(f"\n--- SUMMARY ---")
            formatted_lines.append(f"Total Holdings: {len(holdings)}")
            formatted_lines.append(f"Total Investment: ₹{total_investment:.2f}")
            formatted_lines.append(f"Current Value: ₹{total_current_value:.2f}")
            formatted_lines.append(f"Total P&L: ₹{total_pnl:.2f} ({total_pnl_percent:+.2f}%)")
            
            return "\n".join(formatted_lines)
            
        except Exception as e:
            logger.error(f"Error formatting portfolio data: {e}", exc_info=True)
            return f"Error formatting portfolio data: {str(e)}"
    
    @staticmethod
    def format_returns_for_llm(returns_data: Dict, symbols: Optional[List[str]] = None) -> str:
        """
        Format returns data for LLM
        
        Args:
            returns_data: Dictionary containing returns data
            symbols: Optional list of symbols to filter (from portfolio)
            
        Returns:
            Formatted string for LLM context
        """
        try:
            if not returns_data or returns_data.get("status") != "success":
                return "Returns data not available."
            
            data_list = returns_data.get("data", [])
            if isinstance(data_list, dict):
                # Single stock data
                data_list = [data_list]
            
            if not data_list:
                return "No returns data available."
            
            # Filter by symbols if provided
            if symbols:
                data_list = [
                    d for d in data_list 
                    if d.get("symbol") and d.get("symbol").upper() in [s.upper() for s in symbols]
                ]
            
            # Limit to top 30 stocks to avoid token overflow
            data_list = data_list[:30]
            
            formatted_lines = ["=== STOCK RETURNS DATA ==="]
            
            for stock in data_list:
                symbol = stock.get("symbol", "N/A")
                raw_score = stock.get("raw_score")
                returns_1m = stock.get("returns_1_month")
                returns_3m = stock.get("returns_3_months")
                returns_6m = stock.get("returns_6_months")
                returns_1y = stock.get("returns_1_year")
                latest_close = stock.get("latest_close")
                
                formatted_lines.append(f"\n• {symbol}:")
                if latest_close:
                    formatted_lines.append(f"  Current Price: ₹{latest_close:.2f}")
                if raw_score is not None:
                    formatted_lines.append(f"  Raw Score: {raw_score:.4f}")
                if returns_1m is not None:
                    formatted_lines.append(f"  1M Return: {returns_1m:+.2f}%")
                if returns_3m is not None:
                    formatted_lines.append(f"  3M Return: {returns_3m:+.2f}%")
                if returns_6m is not None:
                    formatted_lines.append(f"  6M Return: {returns_6m:+.2f}%")
                if returns_1y is not None:
                    formatted_lines.append(f"  1Y Return: {returns_1y:+.2f}%")
            
            if len(data_list) > 30:
                formatted_lines.append(f"\n... and {len(data_list) - 30} more stocks")
            
            return "\n".join(formatted_lines)
            
        except Exception as e:
            logger.error(f"Error formatting returns data: {e}")
            return "Error formatting returns data."
    
    @staticmethod
    def format_bhavcopy_for_llm(bhavcopy_data: Dict, symbols: Optional[List[str]] = None) -> str:
        """
        Format bhavcopy data for LLM
        
        Args:
            bhavcopy_data: Dictionary containing bhavcopy data
            symbols: Optional list of symbols to filter (from portfolio)
            
        Returns:
            Formatted string for LLM context
        """
        try:
            if not bhavcopy_data or bhavcopy_data.get("status") != "success":
                return "Bhavcopy data not available."
            
            data_list = bhavcopy_data.get("data", [])
            if isinstance(data_list, dict):
                # Single stock data
                data_list = [data_list]
            
            if not data_list:
                return "No bhavcopy data available."
            
            # Filter by symbols if provided
            if symbols:
                data_list = [
                    d for d in data_list 
                    if d.get("symbol") and d.get("symbol").upper() in [s.upper() for s in symbols]
                ]
            
            # Filter out non-equity instruments (G-Secs, bonds, etc.)
            # Only include equity stocks (series EQ, BE, etc.)
            equity_data = []
            for stock in data_list:
                series = stock.get("series") or stock.get("SERIES", "").upper()
                symbol = stock.get("symbol") or stock.get("SYMBOL", "")
                
                # Skip G-Secs (contain "GS" in symbol) and other non-equity instruments
                if "GS" in symbol.upper() or series not in ["EQ", "BE", "BZ", "B1", "B2"]:
                    continue
                
                # Only include stocks with price data
                close_price = (
                    stock.get("close_price") or 
                    stock.get("CLOSE_PRICE") or 
                    stock.get("close") or 
                    stock.get("CLOSE") or
                    stock.get("last_price") or
                    stock.get("LAST_PRICE")
                )
                
                if close_price:
                    equity_data.append(stock)
            
            # Limit to top 30 equity stocks to avoid token overflow
            equity_data = equity_data[:30]
            
            if not equity_data:
                return "No equity stock data available in bhavcopy."
            
            formatted_lines = ["=== BHAVCOPY MARKET DATA (Equity Stocks) ==="]
            formatted_lines.append(f"Total Stocks: {len(equity_data)}")
            formatted_lines.append("")
            
            for stock in equity_data:
                symbol = stock.get("symbol") or stock.get("SYMBOL", "N/A")
                series = stock.get("series") or stock.get("SERIES", "")
                
                # Get price data (try multiple field names)
                prev_close = (
                    stock.get("prev_close") or 
                    stock.get("PREV_CLOSE") or 
                    stock.get("previous_close") or
                    None
                )
                open_price = (
                    stock.get("open_price") or 
                    stock.get("OPEN_PRICE") or 
                    stock.get("open") or 
                    stock.get("OPEN") or
                    None
                )
                high_price = (
                    stock.get("high_price") or 
                    stock.get("HIGH_PRICE") or 
                    stock.get("high") or 
                    stock.get("HIGH") or
                    None
                )
                low_price = (
                    stock.get("low_price") or 
                    stock.get("LOW_PRICE") or 
                    stock.get("low") or 
                    stock.get("LOW") or
                    None
                )
                close_price = (
                    stock.get("close_price") or 
                    stock.get("CLOSE_PRICE") or 
                    stock.get("close") or 
                    stock.get("CLOSE") or
                    stock.get("last_price") or
                    stock.get("LAST_PRICE") or
                    None
                )
                
                # Get volume data
                volume = (
                    stock.get("total_traded_qty") or 
                    stock.get("TTL_TRD_QNTY") or 
                    stock.get("volume") or 
                    stock.get("VOLUME") or
                    None
                )
                turnover = (
                    stock.get("turnover_lacs") or 
                    stock.get("TURNOVER_LACS") or 
                    stock.get("turnover") or
                    None
                )
                
                # Calculate change
                change = None
                change_percent = None
                if close_price and prev_close:
                    try:
                        change = close_price - prev_close
                        change_percent = (change / prev_close * 100) if prev_close > 0 else 0
                    except (TypeError, ValueError):
                        pass
                
                formatted_lines.append(f"• {symbol} ({series}):")
                if prev_close:
                    formatted_lines.append(f"  Previous Close: ₹{prev_close:.2f}")
                if open_price:
                    formatted_lines.append(f"  Open: ₹{open_price:.2f}")
                if high_price:
                    formatted_lines.append(f"  High: ₹{high_price:.2f}")
                if low_price:
                    formatted_lines.append(f"  Low: ₹{low_price:.2f}")
                if close_price:
                    formatted_lines.append(f"  Close: ₹{close_price:.2f}")
                if change is not None and change_percent is not None:
                    formatted_lines.append(f"  Change: ₹{change:+.2f} ({change_percent:+.2f}%)")
                if volume:
                    formatted_lines.append(f"  Volume: {int(volume):,}")
                if turnover:
                    formatted_lines.append(f"  Turnover: ₹{turnover:.2f} Lacs")
                formatted_lines.append("")
            
            if len(equity_data) > 30:
                formatted_lines.append(f"... and {len(equity_data) - 30} more stocks")
            
            return "\n".join(formatted_lines)
            
        except Exception as e:
            logger.error(f"Error formatting bhavcopy data: {e}", exc_info=True)
            return f"Error formatting bhavcopy data: {str(e)}"
    
    @staticmethod
    def combine_data_context(
        portfolio_data: Optional[str] = None,
        returns_data: Optional[str] = None,
        bhavcopy_data: Optional[str] = None
    ) -> str:
        """
        Combine all data contexts into a single string
        
        Args:
            portfolio_data: Formatted portfolio data
            returns_data: Formatted returns data
            bhavcopy_data: Formatted bhavcopy data
            
        Returns:
            Combined context string
        """
        context_parts = []
        
        if portfolio_data and portfolio_data != "Portfolio data not available.":
            context_parts.append(portfolio_data)
        
        if returns_data and returns_data != "Returns data not available.":
            context_parts.append(returns_data)
        
        if bhavcopy_data and bhavcopy_data != "Bhavcopy data not available.":
            context_parts.append(bhavcopy_data)
        
        if not context_parts:
            return "No trading data available for context."
        
        return "\n\n".join(context_parts)

