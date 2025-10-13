import json
import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import or_, and_, func, text

from app.models.instrument import Instrument
from app.services.iifl_service import IIFLService
from app.core.database import get_db
from typing import Dict, Optional
from loguru import logger
from app.services.iifl_connect import IIFLConnect
from app.models.user import User

logger = logging.getLogger(__name__)

class InstrumentService:
    """Service for managing instrument master data"""

    def __init__(self, db: Session):
        self.db = db
        self.iifl_service = IIFLService(db)

    def download_and_store_instruments(self, user_id: int, exchange_segments: List[str] = None) -> Dict:
        """Download instruments from IIFL and store in database"""
        try:
            if not exchange_segments:
                exchange_segments = ["NSECM", "NSEFO"]  # Default to major segments
            
            logger.info(f"Starting instrument download for segments: {exchange_segments}")
            
            # Download from IIFL
            master_data = self.iifl_service.get_instrument_master(self.db, user_id, exchange_segments)
            
            if master_data.get("type") != "success":
                raise Exception(f"IIFL download failed: {master_data.get('description')}")
            
            raw_instruments = master_data.get("result", [])
            logger.info(f"Downloaded {len(raw_instruments)} raw instruments from IIFL")
            
            # Parse and store instruments
            stored_count = 0
            updated_count = 0
            error_count = 0
            
            # Process in batches for better performance
            batch_size = 1000
            for i in range(0, len(raw_instruments), batch_size):
                batch = raw_instruments[i:i + batch_size]
                batch_result = self._process_instrument_batch(batch)
                
                stored_count += batch_result["stored"]
                updated_count += batch_result["updated"] 
                error_count += batch_result["errors"]
                
                # Commit batch
                self.db.commit()
                
                if (i // batch_size + 1) % 10 == 0:  # Log every 10 batches
                    logger.info(f"Processed {i + len(batch)}/{len(raw_instruments)} instruments")
            
            # Mark old instruments as inactive
            cutoff_time = datetime.utcnow() - timedelta(hours=1)
            inactive_count = self.db.query(Instrument).filter(
                and_(
                    Instrument.last_updated < cutoff_time,
                    Instrument.is_active == True,
                    Instrument.exchange_segment.in_(exchange_segments)
                )
            ).update({"is_active": False})
            
            self.db.commit()
            
            result = {
                "status": "success",
                "exchange_segments": exchange_segments,
                "total_downloaded": len(raw_instruments),
                "stored_new": stored_count,
                "updated_existing": updated_count,
                "marked_inactive": inactive_count,
                "errors": error_count,
                "download_time": datetime.utcnow().isoformat()
            }
            
            logger.info(f"Instrument update complete: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to download and store instruments: {str(e)}")
            self.db.rollback()
            raise

    def _process_instrument_batch(self, raw_instruments: List[str]) -> Dict:
        """Process a batch of raw instrument strings"""
        stored = 0
        updated = 0
        errors = 0
        
        for raw_instrument in raw_instruments:
            try:
                # Parse JSON string
                if isinstance(raw_instrument, str):
                    instrument_data = json.loads(raw_instrument)
                elif isinstance(raw_instrument, dict):
                    instrument_data = raw_instrument
                else:
                    errors += 1
                    continue
                
                exchange_instrument_id = instrument_data.get("ExchangeInstrumentID")
                if not exchange_instrument_id:
                    errors += 1
                    continue
                
                # Check if instrument exists
                existing = self.db.query(Instrument).filter(
                    Instrument.exchange_instrument_id == exchange_instrument_id
                ).first()
                
                if existing:
                    # Update existing instrument
                    self._update_instrument_from_data(existing, instrument_data)
                    updated += 1
                else:
                    # Create new instrument
                    new_instrument = Instrument.from_iifl_data(instrument_data)
                    self.db.add(new_instrument)
                    stored += 1
                    
            except Exception as e:
                logger.warning(f"Failed to process instrument: {str(e)[:100]}")
                errors += 1
                continue
        
        return {"stored": stored, "updated": updated, "errors": errors}

    def _update_instrument_from_data(self, instrument: Instrument, data: dict):
        """Update existing instrument with new data"""
        # Update fields that might change
        instrument.display_name = data.get("DisplayName", instrument.display_name)
        instrument.company_name = data.get("CompanyName", instrument.company_name)
        instrument.series = data.get("Series", instrument.series)
        instrument.lot_size = data.get("LotSize", instrument.lot_size)
        instrument.tick_size = data.get("TickSize", instrument.tick_size)
        instrument.freeze_qty = data.get("FreezeQty", instrument.freeze_qty)
        
        # Update price bands (change daily)
        price_band = data.get("PriceBand", {})
        if isinstance(price_band, dict):
            instrument.price_band_high = price_band.get("High")
            instrument.price_band_low = price_band.get("Low")
        
        # Mark as active and update timestamp
        instrument.is_active = True
        instrument.last_updated = datetime.utcnow()
        instrument.raw_data = str(data)

    def search_instruments(self, query: str, limit: int = 50, 
                          exchange_segments: List[str] = None,
                          instrument_types: List[str] = None) -> List[Dict]:
        """Search instruments in database"""
        try:
            # Build base query
            db_query = self.db.query(Instrument).filter(Instrument.is_active == True)
            
            # Filter by exchange segments
            if exchange_segments:
                db_query = db_query.filter(Instrument.exchange_segment.in_(exchange_segments))
            
            # Filter by instrument types
            if instrument_types:
                db_query = db_query.filter(Instrument.instrument_type.in_(instrument_types))
            
            # Search in name and display_name
            search_term = f"%{query.upper()}%"
            db_query = db_query.filter(
                or_(
                    func.upper(Instrument.name).like(search_term),
                    func.upper(Instrument.display_name).like(search_term),
                    func.upper(Instrument.company_name).like(search_term)
                )
            )
            
            # Order by relevance (exact matches first, then partial)
            db_query = db_query.order_by(
                # Exact name matches first
                func.upper(Instrument.name) == query.upper(),
                # Then exact display name matches
                func.upper(Instrument.display_name) == query.upper(),
                # Then by name length (shorter names first for better relevance)
                func.length(Instrument.name),
                Instrument.name
            )
            
            # Apply limit
            instruments = db_query.limit(limit).all()
            
            return [instrument.to_dict() for instrument in instruments]
            
        except Exception as e:
            logger.error(f"Instrument search failed: {str(e)}")
            return []

    def get_instrument_by_id(self, exchange_instrument_id: int) -> Optional[Dict]:
        """Get specific instrument by ID"""
        try:
            instrument = self.db.query(Instrument).filter(
                and_(
                    Instrument.exchange_instrument_id == exchange_instrument_id,
                    Instrument.is_active == True
                )
            ).first()
            
            return instrument.to_dict() if instrument else None
            
        except Exception as e:
            logger.error(f"Failed to get instrument {exchange_instrument_id}: {str(e)}")
            return None

    def get_instruments_stats(self) -> Dict:
        """Get statistics about stored instruments"""
        try:
            total_active = self.db.query(func.count(Instrument.id)).filter(
                Instrument.is_active == True
            ).scalar()
            
            total_inactive = self.db.query(func.count(Instrument.id)).filter(
                Instrument.is_active == False
            ).scalar()
            
            # Count by exchange segment
            exchange_stats = self.db.query(
                Instrument.exchange_segment,
                func.count(Instrument.id).label('count')
            ).filter(
                Instrument.is_active == True
            ).group_by(Instrument.exchange_segment).all()
            
            # Count by instrument type
            type_stats = self.db.query(
                Instrument.instrument_type,
                func.count(Instrument.id).label('count')
            ).filter(
                Instrument.is_active == True
            ).group_by(Instrument.instrument_type).all()
            
            # Latest update time
            latest_update = self.db.query(func.max(Instrument.last_updated)).scalar()
            
            return {
                "total_active_instruments": total_active,
                "total_inactive_instruments": total_inactive,
                "exchange_segments": {stat.exchange_segment: stat.count for stat in exchange_stats},
                "instrument_types": {stat.instrument_type: stat.count for stat in type_stats},
                "latest_update": latest_update.isoformat() if latest_update else None
            }
            
        except Exception as e:
            logger.error(f"Failed to get instrument stats: {str(e)}")
            return {}

    def cleanup_old_instruments(self, days_old: int = 7) -> int:
        """Remove instruments that haven't been updated for specified days"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)
            
            deleted_count = self.db.query(Instrument).filter(
                and_(
                    Instrument.last_updated < cutoff_date,
                    Instrument.is_active == False
                )
            ).delete()
            
            self.db.commit()
            
            logger.info(f"Cleaned up {deleted_count} old instruments")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup old instruments: {str(e)}")
            self.db.rollback()
            return 0

def get_instrument_service(db: Session) -> InstrumentService:
    """Dependency to get instrument service"""
    return InstrumentService(db) 

class InstrumentMappingService:
    """Service to map instrument IDs to stock names and other details"""
    
    def __init__(self):
        self.instrument_cache: Dict[int, Dict] = {}
    
    async def get_stock_info_by_instrument_id(self, instrument_id: int, user: User) -> Optional[Dict]:
        """Get stock information by instrument ID using IIFL search"""
        try:
            # Check cache first
            if instrument_id in self.instrument_cache:
                return self.instrument_cache[instrument_id]
            
            # Initialize IIFL Connect for market data
            iifl_client = IIFLConnect(user, api_type="market")
            
            # Login to get token
            login_response = iifl_client.marketdata_login()
            if login_response.get("type") != "success":
                logger.error(f"Failed to login to IIFL Market Data API for instrument {instrument_id}")
                return None
            
            # Search for instruments by ID
            search_response = iifl_client.search_by_instrument_id(instrument_id)
            
            # Logout
            iifl_client.marketdata_logout()
            
            if search_response.get("type") == "success" and search_response.get("result"):
                instruments = search_response["result"]
                if instruments:
                    # Get the first matching instrument
                    instrument = instruments[0]
                    
                    stock_info = {
                        "symbol": instrument.get("Name", f"Unknown_{instrument_id}"),
                        "name": instrument.get("Description", f"Unknown Instrument {instrument_id}"),
                        "exchange_segment": instrument.get("ExchangeSegment"),
                        "series": instrument.get("Series", ""),
                        "isin": instrument.get("ISIN", ""),
                        "lot_size": instrument.get("LotSize", 1),
                        "tick_size": instrument.get("TickSize", 0.01)
                    }
                    
                    # Cache the result
                    self.instrument_cache[instrument_id] = stock_info
                    logger.info(f"Found stock info for instrument {instrument_id}: {stock_info['symbol']}")
                    
                    return stock_info
            
            logger.warning(f"No stock found for instrument ID {instrument_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting stock info for instrument {instrument_id}: {str(e)}")
            return None
    
    def get_cached_stock_info(self, instrument_id: int) -> Optional[Dict]:
        """Get cached stock information"""
        return self.instrument_cache.get(instrument_id)
    
    def clear_cache(self):
        """Clear the instrument cache"""
        self.instrument_cache.clear()
        logger.info("Instrument cache cleared") 