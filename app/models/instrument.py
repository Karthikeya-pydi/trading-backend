from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, Index
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from app.core.database import Base

class Instrument(Base):
    """IIFL Instrument Master Data Model"""
    __tablename__ = "instruments"

    # Primary identifiers
    id = Column(Integer, primary_key=True, index=True)
    exchange_instrument_id = Column(Integer, unique=True, nullable=False, index=True)
    
    # Basic information
    name = Column(String(50), nullable=False, index=True)  # Symbol like "RELIANCE", "NIFTY"
    display_name = Column(String(200), nullable=False, index=True)  # Full name
    company_name = Column(String(200), nullable=True)
    
    # Market details
    exchange_segment = Column(String(10), nullable=False, index=True)  # NSECM, NSEFO, etc.
    instrument_type = Column(String(20), nullable=False, index=True)   # EQUITY, INDEX, FUTIDX, OPTIDX
    series = Column(String(10), nullable=True, index=True)             # EQ, BE, etc.
    
    # Trading specifications
    lot_size = Column(Integer, nullable=False, default=1)
    tick_size = Column(Float, nullable=False, default=0.05)
    freeze_qty = Column(Integer, nullable=True)
    
    # Price bands (updated daily)
    price_band_high = Column(Float, nullable=True)
    price_band_low = Column(Float, nullable=True)
    
    # F&O specific fields
    expiry_date = Column(DateTime, nullable=True)
    strike_price = Column(Float, nullable=True)
    option_type = Column(String(2), nullable=True)  # CE, PE
    
    # Metadata
    is_active = Column(Boolean, default=True, nullable=False)
    last_updated = Column(DateTime, default=datetime.utcnow, nullable=False)
    raw_data = Column(Text, nullable=True)  # Store original JSON for reference
    
    # Create composite indexes for fast searching
    __table_args__ = (
        Index('idx_name_search', 'name'),
        Index('idx_display_name_search', 'display_name'),
        Index('idx_exchange_type', 'exchange_segment', 'instrument_type'),
        Index('idx_active_instruments', 'is_active', 'exchange_segment'),
        Index('idx_expiry_search', 'expiry_date', 'instrument_type'),
    )

    def __repr__(self):
        return f"<Instrument(id={self.exchange_instrument_id}, name='{self.name}', display_name='{self.display_name}')>"

    def to_dict(self):
        """Convert to dictionary for API responses"""
        return {
            "exchange_instrument_id": self.exchange_instrument_id,
            "name": self.name,
            "display_name": self.display_name,
            "company_name": self.company_name,
            "exchange_segment": self.exchange_segment,
            "instrument_type": self.instrument_type,
            "series": self.series,
            "lot_size": self.lot_size,
            "tick_size": self.tick_size,
            "freeze_qty": self.freeze_qty,
            "price_band_high": self.price_band_high,
            "price_band_low": self.price_band_low,
            "expiry_date": self.expiry_date.isoformat() if self.expiry_date else None,
            "strike_price": self.strike_price,
            "option_type": self.option_type,
            "is_active": self.is_active,
            "last_updated": self.last_updated.isoformat()
        }

    @classmethod
    def from_iifl_data(cls, iifl_instrument_data: dict):
        """Create Instrument from IIFL JSON data"""
        from datetime import datetime
        
        # Parse expiry date if present
        expiry_date = None
        if iifl_instrument_data.get("ExpiryDate"):
            try:
                expiry_str = iifl_instrument_data["ExpiryDate"]
                # Handle different date formats: "30-Jan-2025", "2025-01-30", etc.
                for fmt in ["%d-%b-%Y", "%Y-%m-%d", "%d/%m/%Y"]:
                    try:
                        expiry_date = datetime.strptime(expiry_str, fmt)
                        break
                    except ValueError:
                        continue
            except:
                pass
        
        # Extract price band
        price_band = iifl_instrument_data.get("PriceBand", {})
        
        return cls(
            exchange_instrument_id=iifl_instrument_data.get("ExchangeInstrumentID"),
            name=iifl_instrument_data.get("Name", ""),
            display_name=iifl_instrument_data.get("DisplayName", ""),
            company_name=iifl_instrument_data.get("CompanyName"),
            exchange_segment=iifl_instrument_data.get("ExchangeSegment", ""),
            instrument_type=iifl_instrument_data.get("InstrumentType", ""),
            series=iifl_instrument_data.get("Series"),
            lot_size=iifl_instrument_data.get("LotSize", 1),
            tick_size=iifl_instrument_data.get("TickSize", 0.05),
            freeze_qty=iifl_instrument_data.get("FreezeQty"),
            price_band_high=price_band.get("High") if isinstance(price_band, dict) else None,
            price_band_low=price_band.get("Low") if isinstance(price_band, dict) else None,
            expiry_date=expiry_date,
            strike_price=iifl_instrument_data.get("StrikePrice"),
            option_type=iifl_instrument_data.get("OptionType"),
            is_active=True,
            last_updated=datetime.utcnow(),
            raw_data=str(iifl_instrument_data)  # Store original data
        ) 