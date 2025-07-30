import json
from datetime import datetime
from typing import Dict, List
from sqlalchemy.orm import Session
from loguru import logger

from app.models.user import User
from app.services.iifl_connect import IIFLConnect

class HoldingsMarketDataService:
    """
    Simple service to get current market prices for holdings and calculate P&L
    """
    
    def __init__(self, user: User, db: Session):
        self.user = user
        self.db = db
        self.iifl_client = None
    
    def get_holdings_with_current_prices(self) -> Dict:
        """
        Get holdings with current market prices and calculate P&L
        P&L = (current_price - avg_price) * quantity
        """
        try:
            # Step 1: Get holdings from IIFL Interactive API
            holdings_data = self._get_holdings_from_iifl()
            if not holdings_data:
                return {
                    "status": "failed",
                    "error": "No holdings found or failed to fetch holdings"
                }
            
            # Step 2: Get current market prices for all holdings
            holdings_with_prices = []
            total_investment = 0
            total_current_value = 0
            
            for holding in holdings_data:
                isin = holding.get("ISIN")
                quantity = holding.get("HoldingQuantity", 0)
                avg_price = holding.get("BuyAvgPrice", 0)
                nse_instrument_id = holding.get("ExchangeNSEInstrumentId")
                
                # Calculate investment value
                investment_value = quantity * avg_price
                total_investment += investment_value
                
                # Get current market price
                current_price = avg_price  # Default to avg_price if we can't get market price
                if nse_instrument_id:
                    current_price = self._get_current_price(nse_instrument_id)
                
                # Calculate current value and P&L
                current_value = quantity * current_price
                pnl = current_value - investment_value
                pnl_percent = (pnl / investment_value * 100) if investment_value > 0 else 0
                
                total_current_value += current_value
                
                holdings_with_prices.append({
                    "stock_name": self._get_stock_name(isin, nse_instrument_id),
                    "isin": isin,
                    "quantity": quantity,
                    "avg_price": avg_price,
                    "current_price": current_price,
                    "invested_value": investment_value,
                    "market_value": current_value,
                    "pnl": pnl,
                    "pnl_percent": pnl_percent,
                    "type": "Collateral" if holding.get("IsCollateralHolding", False) else "Regular",
                    "nse_instrument_id": nse_instrument_id
                })
            
            # Calculate total P&L
            total_pnl = total_current_value - total_investment
            total_pnl_percent = (total_pnl / total_investment * 100) if total_investment > 0 else 0
            
            return {
                "status": "success",
                "holdings": holdings_with_prices,
                "summary": {
                    "total_holdings": len(holdings_with_prices),
                    "total_investment": round(total_investment, 2),
                    "total_current_value": round(total_current_value, 2),
                    "total_pnl": round(total_pnl, 2),
                    "total_pnl_percent": round(total_pnl_percent, 2)
                },
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Failed to get holdings with current prices: {e}")
            return {
                "status": "failed",
                "error": str(e)
            }
    
    def _get_holdings_from_iifl(self) -> List[Dict]:
        """Get holdings from IIFL Interactive API"""
        try:
            # Use IIFL Interactive API to get holdings
            interactive_client = IIFLConnect(self.user, api_type="interactive")
            login_response = interactive_client.interactive_login()
            
            if login_response.get("type") != "success":
                logger.error("Failed to login to IIFL Interactive API")
                return []
            
            # Get holdings (note: method is get_holding, not get_holdings)
            holdings_response = interactive_client.get_holding()
            interactive_client.interactive_logout()
            
            if holdings_response.get("type") == "success":
                rms_holdings = holdings_response.get("result", {}).get("RMSHoldings", {}).get("Holdings", {})
                return list(rms_holdings.values())
            
            return []
            
        except Exception as e:
            logger.error(f"Failed to get holdings from IIFL: {e}")
            return []
    
    def _get_current_price(self, nse_instrument_id: int) -> float:
        """Get current market price for a given NSE instrument ID"""
        try:
            if not self.iifl_client:
                self.iifl_client = IIFLConnect(self.user, api_type="market")
                login_response = self.iifl_client.marketdata_login()
                if login_response.get("type") != "success":
                    logger.error("Failed to login to IIFL Market Data API")
                    return 0.0
            
            # Get quote for the instrument
            instruments = [{
                "exchangeSegment": 1,  # NSECM
                "exchangeInstrumentID": nse_instrument_id
            }]
            
            quote_response = self.iifl_client.get_quote(
                Instruments=instruments,
                xtsMessageCode=1512,  # LTP data
                publishFormat="JSON"
            )
            
            if quote_response.get("type") == "success":
                result = quote_response.get("result", {})
                quotes = result.get("listQuotes", result.get("quotesList", []))
                
                if quotes:
                    quote_str = quotes[0]
                    if isinstance(quote_str, str):
                        quote = json.loads(quote_str)
                    else:
                        quote = quote_str
                    
                    current_price = float(quote.get("LastTradedPrice", 0))
                    return current_price
            
            return 0.0
            
        except Exception as e:
            logger.error(f"Failed to get current price for instrument {nse_instrument_id}: {e}")
            return 0.0
    
    def _get_stock_name(self, isin: str, nse_instrument_id: int = None) -> str:
        """Get stock name from ISIN or NSE instrument ID"""
        try:
            # First try to get stock name using NSE instrument ID
            if nse_instrument_id and self.iifl_client:
                stock_name = self._get_stock_name_by_instrument_id(nse_instrument_id)
                if stock_name and stock_name != f"Stock-{isin[:6]}":
                    return stock_name
            
            # Fallback to ISIN mapping
            isin_to_name = {
                "INE548A01028": "HFCL Limited",
                "INE002A01018": "Reliance Industries",
                "INE467B01029": "TCS Limited",
                "INE040A01034": "HDFC Bank",
                "INE009A01021": "Infosys Limited",
                "INE204C01028": "Infosys Limited",
                "INE732X01018": "Reliance Industries"
            }
            return isin_to_name.get(isin, f"Stock-{isin[:6]}")
            
        except Exception as e:
            logger.error(f"Failed to get stock name for ISIN {isin}: {e}")
            return f"Stock-{isin[:6]}"
    
    def _get_stock_name_by_instrument_id(self, nse_instrument_id: int) -> str:
        """Get stock name by searching IIFL with instrument ID"""
        try:
            if not self.iifl_client:
                return None
            
            # Search for the instrument using the instrument ID
            search_response = self.iifl_client.search_by_instrumentid([{
                "exchangeSegment": 1,  # NSECM
                "exchangeInstrumentID": nse_instrument_id
            }])
            
            if search_response.get("type") == "success" and search_response.get("result"):
                instruments = search_response["result"]
                if instruments:
                    # Get the first result
                    instrument = instruments[0]
                    return instrument.get("DisplayName", instrument.get("Name"))
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get stock name by instrument ID {nse_instrument_id}: {e}")
            return None
    
    def cleanup(self):
        """Clean up IIFL connections"""
        try:
            if self.iifl_client:
                self.iifl_client.marketdata_logout()
                self.iifl_client = None
        except Exception as e:
            logger.error(f"Error cleaning up IIFL connections: {e}") 