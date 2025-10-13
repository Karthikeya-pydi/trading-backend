import http.client
from io import StringIO
import pandas as pd
import os
import time
import json
import sys
from pathlib import Path

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

# Local storage path for nifty indices data
NIFTY_STORAGE_PATH = Path("uploads/nifty_indices")

url_dict = {'Nifty 50': '/IndexConstituent/ind_nifty50list.csv',
 'Nifty Next 50': '/IndexConstituent/ind_niftynext50list.csv',
 'Nifty 100': '/IndexConstituent/ind_nifty100list.csv',
 'Nifty 200': '/IndexConstituent/ind_nifty200list.csv',
 'Nifty Total Market': '/IndexConstituent/ind_niftytotalmarket_list.csv',
 'Nifty 500': '/IndexConstituent/ind_nifty500list.csv',
 'Nifty500 Multicap 50:25:25': '/IndexConstituent/ind_nifty500Multicap502525_list.csv',
 'Nifty500 LargeMidSmall Equal-Cap Weighted': '/IndexConstituent/ind_nifty500LargeMidSmallEqualCapWeighted_list.csv',
 'Nifty Midcap150': '/IndexConstituent/ind_niftymidcap150list.csv',
 'Nifty Midcap 50': '/IndexConstituent/ind_niftymidcap50list.csv',
 'Nifty Midcap Select': '/IndexConstituent/ind_niftymidcapselect_list.csv',
 'Nifty Midcap 100': '/IndexConstituent/ind_niftymidcap100list.csv',
 'Nifty Smallcap 250': '/IndexConstituent/ind_niftysmallcap250list.csv',
 'Nifty Smallcap 50': '/IndexConstituent/ind_niftysmallcap50list.csv',
 'Nifty Smallcap 100': '/IndexConstituent/ind_niftysmallcap100list.csv',
 'Nifty Microcap 250': '/IndexConstituent/ind_niftymicrocap250_list.csv',
 'Nifty LargeMidcap 250': '/IndexConstituent/ind_niftylargemidcap250list.csv',
 'Nifty MidSmallcap 400': '/IndexConstituent/ind_niftymidsmallcap400list.csv',
 'Nifty India FPI 150': '/IndexConstituent/ind_niftyIndiaFPI150_list.csv',
 'Nifty Auto': '/IndexConstituent/ind_niftyautolist.csv',
 'Nifty Bank': '/IndexConstituent/ind_niftybanklist.csv',
 'Nifty Chemicals': '/IndexConstituent/ind_niftyChemicals_list.csv',
 'Nifty Financial Services': '/IndexConstituent/ind_niftyfinancelist.csv',
 'Nifty Financial Services 25/50': '/IndexConstituent/ind_niftyfinancialservices25-50list.csv',
 'Nifty FMCG': '/IndexConstituent/ind_niftyfmcglist.csv',
 'Nifty Healthcare': '/IndexConstituent/ind_niftyhealthcarelist.csv',
 'Nifty IT': '/IndexConstituent/ind_niftyitlist.csv',
 'Nifty Media': '/IndexConstituent/ind_niftymedialist.csv',
 'Nifty Metal': '/IndexConstituent/ind_niftymetallist.csv',
 'Nifty Pharma': '/IndexConstituent/ind_niftypharmalist.csv',
 'Nifty Private Bank': '/IndexConstituent/ind_nifty_privatebanklist.csv',
 'Nifty PSU Bank': '/IndexConstituent/ind_niftypsubanklist.csv',
 'Nifty Realty': '/IndexConstituent/ind_niftyrealtylist.csv',
 'Nifty Consumer Durables': '/IndexConstituent/ind_niftyconsumerdurableslist.csv',
 'Nifty Oil and Gas': '/IndexConstituent/ind_niftyoilgaslist.csv',
 'Nifty500 Healthcare': '/IndexConstituent/ind_nifty500Healthcare_list.csv',
 'Nifty MidSmall Financial Services': '/IndexConstituent/ind_niftymidsmallfinancailservice_list.csv',
 'Nifty MidSmall Healthcare': '/IndexConstituent/ind_niftymidsmallhealthcare_list.csv',
 'Nifty MidSmall IT & Telecom': '/IndexConstituent/ind_niftymidsmallitAndtelecom_list.csv'}

def fetch_and_save_index(index_name, url_path):
    """Fetch index data and save to Redis"""
    try:
        conn = http.client.HTTPSConnection("niftyindices.com")
        payload = ''
        headers = {}
        
        conn.request("GET", url_path, payload, headers)
        res = conn.getresponse()
        
        if res.status == 200:
            data = res.read()
            df = pd.read_csv(StringIO(data.decode("utf-8")))
            
            # Ensure storage directory exists
            NIFTY_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
            
            # Create safe filename
            safe_filename = index_name.replace(' ', '_').replace('&', 'and').replace('/', '_')
            csv_file_path = NIFTY_STORAGE_PATH / f"{safe_filename}.csv"
            
            # Save DataFrame to CSV file
            df.to_csv(csv_file_path, index=False)
            
            print(f"✓ {index_name}: {len(df)} stocks saved to CSV file '{csv_file_path}'")
            return True
            
        else:
            print(f"✗ {index_name}: HTTP {res.status} - {res.reason}")
            return False
            
    except Exception as e:
        print(f"✗ {index_name}: Error - {str(e)}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()

def get_index_from_storage(index_name):
    """Retrieve index data from local CSV file"""
    try:
        # Create safe filename
        safe_filename = index_name.replace(' ', '_').replace('&', 'and').replace('/', '_')
        csv_file_path = NIFTY_STORAGE_PATH / f"{safe_filename}.csv"
        
        if csv_file_path.exists():
            df = pd.read_csv(csv_file_path)
            print(f"✓ Retrieved {index_name} from CSV: {len(df)} stocks")
            return df
        else:
            print(f"✗ {index_name} not found in local storage")
            return None
            
    except Exception as e:
        print(f"✗ Error retrieving {index_name}: {str(e)}")
        return None

def list_all_indices_in_storage():
    """List all available indices in local storage"""
    try:
        # Ensure storage directory exists
        NIFTY_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
        
        # Scan for CSV files
        csv_files = list(NIFTY_STORAGE_PATH.glob("*.csv"))
        
        if csv_files:
            print("Available indices in local storage:")
            for csv_file in csv_files:
                index_name = csv_file.stem.replace('_', ' ')
                print(f"  - {index_name}")
        else:
            print("No indices found in local storage")
            
    except Exception as e:
        print(f"✗ Error listing indices from local storage: {str(e)}")

def delete_index_from_storage(index_name):
    """Delete specific index data from local storage"""
    try:
        # Create safe filename
        safe_filename = index_name.replace(' ', '_').replace('&', 'and').replace('/', '_')
        csv_file_path = NIFTY_STORAGE_PATH / f"{safe_filename}.csv"
        
        if csv_file_path.exists():
            csv_file_path.unlink()
            print(f"✓ Deleted {index_name} from local storage")
            return True
        else:
            print(f"✗ {index_name} not found in local storage")
            return False
            
    except Exception as e:
        print(f"✗ Error deleting {index_name}: {str(e)}")
        return False

def clear_all_nifty_data():
    """Clear all nifty index data from local storage"""
    try:
        # Ensure storage directory exists
        NIFTY_STORAGE_PATH.mkdir(parents=True, exist_ok=True)
        
        # Scan for CSV files
        csv_files = list(NIFTY_STORAGE_PATH.glob("*.csv"))
        
        if csv_files:
            deleted_count = 0
            for csv_file in csv_files:
                try:
                    csv_file.unlink()
                    deleted_count += 1
                except Exception:
                    pass
            
            print(f"✓ Cleared {deleted_count} indices from local storage")
            return deleted_count
        else:
            print("No nifty indices found in local storage to clear")
            return 0
            
    except Exception as e:
        print(f"✗ Error clearing nifty data from local storage: {str(e)}")
        return 0

def main():
    print("Starting to fetch Nifty index data and save to local CSV files...")
    print("=" * 60)
    
    successful_fetches = 0
    total_indices = len(url_dict)
    
    for i, (key, value) in enumerate(url_dict.items(), 1):
        print(f"[{i}/{total_indices}] Fetching {key}...")
        
        if fetch_and_save_index(key, value):
            successful_fetches += 1
        
        # Add a small delay to avoid overwhelming the server
        if i < total_indices:
            time.sleep(0.5)
    
    print("=" * 60)
    print(f"Fetching completed! {successful_fetches}/{total_indices} indices successfully saved to local CSV files.")
    print(f"All data is stored in folder: {NIFTY_STORAGE_PATH}")
    
    # Show available indices in local storage
    print("\n" + "=" * 60)
    list_all_indices_in_storage()

if __name__ == "__main__":
    main()


 