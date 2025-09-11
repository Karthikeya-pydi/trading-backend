import h5py
import pandas as pd
import numpy as np

def examine_h5_file(filename):
    """Examine the structure of an H5 file"""
    print(f"Examining {filename}...")
    
    with h5py.File(filename, 'r') as f:
        print("Keys in H5 file:")
        print(list(f.keys()))
        print("\nStructure:")
        
        def print_structure(name, obj):
            print(f"{name}: {type(obj)}")
            if hasattr(obj, 'shape'):
                print(f"  Shape: {obj.shape}")
            if hasattr(obj, 'dtype'):
                print(f"  Data type: {obj.dtype}")
            print()
        
        f.visititems(print_structure)

def convert_h5_to_csv(h5_filename, csv_filename=None):
    """Convert H5 file to CSV - handles pandas HDF5 format"""
    if csv_filename is None:
        csv_filename = h5_filename.replace('.h5', '.csv')
    
    print(f"Converting {h5_filename} to {csv_filename}...")
    
    try:
        # First try to read as pandas HDF5 file
        df = pd.read_hdf(h5_filename)
        print(f"Successfully read as pandas HDF5 file")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"Index: {df.index.name if df.index.name else 'RangeIndex'}")
        
        # Save to CSV
        df.to_csv(csv_filename, index=True)
        print(f"Successfully converted to {csv_filename}")
        
    except Exception as e:
        print(f"Failed to read as pandas HDF5: {e}")
        print("Trying manual reconstruction...")
        
        # Manual reconstruction for complex HDF5 structures
        with h5py.File(h5_filename, 'r') as f:
            # Look for the main data group (usually 'stage' for pandas)
            if 'stage' in f:
                stage = f['stage']
                
                # Get column names from block items
                columns = []
                for key in stage.keys():
                    if 'items' in key:
                        items = stage[key][:]
                        if items.dtype.kind == 'S':  # String type
                            items = [item.decode('utf-8') for item in items]
                        columns.extend(items)
                
                # Get the main data values
                data_blocks = []
                for key in stage.keys():
                    if 'values' in key:
                        values = stage[key][:]
                        if values.ndim == 2:
                            data_blocks.append(values)
                        elif values.ndim == 1:
                            data_blocks.append(values.reshape(-1, 1))
                
                if data_blocks:
                    # Combine all data blocks
                    combined_data = np.hstack(data_blocks)
                    
                    # Create DataFrame
                    df = pd.DataFrame(combined_data, columns=columns)
                    
                    # Save to CSV
                    df.to_csv(csv_filename, index=False)
                    print(f"Successfully converted to {csv_filename}")
                    print(f"Shape: {df.shape}")
                    print(f"Columns: {list(df.columns)}")
                else:
                    print("No data blocks found to convert")
            else:
                print("No 'stage' group found in HDF5 file")

if __name__ == "__main__":
    h5_file = "Our_Nseadjprice.h5"
    
    # First examine the file
    examine_h5_file(h5_file)
    
    # Then convert it
    convert_h5_to_csv(h5_file)
