#!/usr/bin/env python3
"""
Get ALL Instruments from ALL NSE Exchanges
"""
import requests
import json

# Update with your current JWT token
JWT_TOKEN = "YOUR_JWT_TOKEN_HERE"

headers = {
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Content-Type": "application/json"
}

def get_all_instruments():
    """Get ALL instruments from ALL exchanges"""
    print("🚀 FETCHING ALL INSTRUMENTS FROM ALL EXCHANGES")
    print("=" * 60)
    
    # Get all instruments from all exchanges
    url = "http://localhost:8000/api/market/instruments/master"
    params = {
        "exchange_segments": "NSECM,NSEFO,NSECD,NSECO,BSECM,BSEFO,BSECD",  # ALL NSE + BSE exchanges
        "full_data": "true",  # Get complete dataset
        "include_sample": "false"  # Don't need sample, we want full data
    }
    
    try:
        print("📡 Fetching from ALL exchanges (NSE: NSECM, NSEFO, NSECD, NSECO + BSE: BSECM, BSEFO, BSECD)...")
        print("⏳ This may take 30-60 seconds for complete dataset...")
        
        response = requests.get(url, headers=headers, params=params, timeout=120)
        
        if response.status_code == 200:
            result = response.json()
            
            print(f"✅ SUCCESS! Got complete instrument dataset:")
            print(f"   📊 Total Instruments: {result.get('total_instruments', 0):,}")
            print(f"   📈 Exchange Segments: {', '.join(result.get('exchange_segments', []))}")
            print(f"   🎯 Success Rate: {result.get('success_rate', '0%')}")
            print(f"   🔧 Parse Errors: {result.get('parse_errors', 0)}")
            
            instruments = result.get('instruments', [])
            if instruments:
                print(f"\n📋 Sample from your {len(instruments):,} instruments:")
                
                # Show sample by exchange
                exchanges = {}
                for inst in instruments[:50]:  # First 50 for sample
                    exchange = inst.get('exchange', 'Unknown')
                    if exchange not in exchanges:
                        exchanges[exchange] = []
                    exchanges[exchange].append(inst)
                
                for exchange, inst_list in exchanges.items():
                    print(f"\n   🏢 {exchange}:")
                    for i, inst in enumerate(inst_list[:3]):  # Show 3 per exchange
                        print(f"      {i+1}. {inst.get('name')} ({inst.get('instrument_id')})")
                
                # Show breakdown by instrument types
                print(f"\n📊 Instrument Type Breakdown:")
                series_count = {}
                for inst in instruments:
                    series = inst.get('series', 'Unknown')
                    series_count[series] = series_count.get(series, 0) + 1
                
                for series, count in sorted(series_count.items(), key=lambda x: x[1], reverse=True)[:10]:
                    print(f"      {series}: {count:,} instruments")
                
                return result
            else:
                print("❌ No instruments returned")
                return None
                
        else:
            print(f"❌ Failed: {response.status_code}")
            print(f"   Error: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def get_exchange_breakdown():
    """Get breakdown by individual exchanges"""
    print(f"\n🔍 INDIVIDUAL EXCHANGE BREAKDOWN")
    print("=" * 60)
    
    exchanges = {
        "NSECM": "NSE Capital Market (Cash/Equity)",
        "NSEFO": "NSE Futures & Options", 
        "NSECD": "NSE Currency Derivatives",
        "NSECO": "NSE Commodity Derivatives",
        "BSECM": "BSE Capital Market (Cash/Equity)",
        "BSEFO": "BSE Futures & Options",
        "BSECD": "BSE Currency Derivatives"
    }
    
    total_instruments = 0
    
    for exchange, description in exchanges.items():
        print(f"\n📈 {exchange} - {description}")
        
        url = "http://localhost:8000/api/market/instruments/master"
        params = {
            "exchange_segments": exchange,
            "full_data": "false",  # Just get counts, not full data
            "include_sample": "true"
        }
        
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                result = response.json()
                count = result.get('total_instruments', 0)
                total_instruments += count
                print(f"   📊 {count:,} instruments")
                
                # Show sample
                samples = result.get('sample_instruments', [])
                if samples:
                    print(f"   📋 Sample instruments:")
                    for i, inst in enumerate(samples[:3]):
                        print(f"      {i+1}. {inst.get('name')} ({inst.get('series')})")
            else:
                print(f"   ❌ Failed to fetch {exchange}")
                
        except Exception as e:
            print(f"   ❌ Error fetching {exchange}: {e}")
    
    print(f"\n🎉 TOTAL ACROSS ALL EXCHANGES: {total_instruments:,} instruments")

if __name__ == "__main__":
    # First get breakdown by exchange
    get_exchange_breakdown()
    
    print(f"\n" + "="*80)
    
    # Then get all instruments at once
    all_instruments = get_all_instruments()
    
    if all_instruments:
        print(f"\n🎯 SUCCESS! You now have access to ALL {all_instruments.get('total_instruments', 0):,} instruments!")
        print(f"💡 Use 'full_data=true' to get the complete dataset in your application") 