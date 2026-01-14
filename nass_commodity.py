import pandas as pd
import numpy as np
import glob
import os

# ==========================================
# 1. ENVIRONMENT SETUP
# ==========================================
# This block ensures the script always looks for files in the same folder 
# where this script is saved, regardless of where you run it from.
try:
    script_folder = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_folder)
except:
    pass # Fallback for interactive environments

def clean_nass_file(file_path):
    """
    Process a single NASS Commodity CSV file.
    
    Logic Applied:
    1. Filter: Keeps only rows measured in Dollars ($).
    2. Filter: Keeps only 'SALES' or 'SALES DISTRIBUTION' statistics.
    3. Deduplication: If both exist, prioritizes 'SALES' to avoid double counting.
    4. Time Series: Expands timeline to 1975-2024.
    5. Imputation: Backfills missing years using the next available Census data.
    """
    try:
        # low_memory=False is used because NASS files often mix text/numbers in the same column ex: (D) and (Z)
        df = pd.read_csv(file_path, low_memory=False)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()
    
    # ---------------------------------------------------------
    # STEP 1: FILTER BY UNIT (Column I)
    # ---------------------------------------------------------
    # strictly require 'unit_desc' to be '$'. 
    # Any row measuring Head, Acres, or Lbs is discarded.
    if 'unit_desc' in df.columns:
        df = df[df['unit_desc'] == '$']
    else:
        return pd.DataFrame() # Skip file if standard columns are missing
    
    # ---------------------------------------------------------
    # STEP 2: FILTER BY STATISTIC (Column H)
    # ---------------------------------------------------------
    # only want Sales figures.
    target_stats = ['SALES', 'SALES DISTRIBUTION']
    if 'statisticcat_desc' in df.columns:
        df = df[df['statisticcat_desc'].isin(target_stats)]
        
        # -----------------------------------------------------
        # STEP 3: HANDLE DOUBLE COUNTING
        # -----------------------------------------------------
        # Some commodities list both "SALES" and "SALES DISTRIBUTION".
        # prefer "SALES". Sorting Ascending places 'SALES' before 'SALES DISTRIBUTION'.
        # keep the 'first' occurrence for each State/Year/Commodity.
        df = df.sort_values('statisticcat_desc', ascending=True)
        
        cols_for_dupe = ['state_name', 'year', 'commodity_desc']
        if all(c in df.columns for c in cols_for_dupe):
            df = df.drop_duplicates(subset=cols_for_dupe, keep='first')

    # ---------------------------------------------------------
    # STEP 4: CLEAN THE SALES VALUES (Column AL)
    # ---------------------------------------------------------
    # Raw NASS data contains commas ("1,000") and privacy codes like "(D)" or "(Z)".
    # - remove commas.
    # - force data to numeric, turning "(D)" and "(Z)" into NaN (Blank).
    if 'Value' in df.columns:
        df['Sales'] = df['Value'].astype(str).str.replace(',', '', regex=False)
        df['Sales'] = pd.to_numeric(df['Sales'], errors='coerce')
    else:
        df['Sales'] = np.nan

    # ---------------------------------------------------------
    # STEP 5: EXPAND & BACKFILL TIME SERIES (1975-2024)
    # ---------------------------------------------------------
    # NASS Census data appears every 5 years (e.g., 2002, 2007).
    #  create a full timeline and fill the gaps.
    all_years = pd.DataFrame({'year': range(1975, 2025)})
    processed_chunks = []
    
    # process each commodity/state group individually to prevent data leaking between groups.
    for (state, commodity), group in df.groupby(['state_name', 'commodity_desc']):
        
        # Merge the group onto the full 1975-2024 timeline
        merged = pd.merge(all_years, group, on='year', how='left')
        
        # Restore the State and Commodity names for the newly created blank rows
        merged['state_name'] = state
        merged['commodity_desc'] = commodity
        
        # Backfill Metadata columns (Units, Statistic) so they aren't blank in the new rows.
        # .bfill().ffill() ensures the text description propagates to all years.
        meta_cols = ['short_desc', 'statisticcat_desc']
        for col in meta_cols:
            if col in merged.columns:
                merged[col] = merged[col].bfill().ffill()
        
        # Backfill Sales Data (The Census Logic)
        # Rule: "If census is 2005, 2001-2004 will be data from 2005"
        # .bfill() takes the future value and pulls it backwards to fill gaps.
        merged['Sales'] = merged['Sales'].bfill()
        
        # Ensure we restrict the final output to exactly 1975-2024
        merged = merged[(merged['year'] >= 1975) & (merged['year'] <= 2024)]
        
        processed_chunks.append(merged)
        
    if not processed_chunks:
        return pd.DataFrame()
        
    final_df = pd.concat(processed_chunks, ignore_index=True)
    
    # ---------------------------------------------------------
    # STEP 6: FINAL FORMATTING
    # ---------------------------------------------------------
    # Rename Column J ('short_desc') to 'units'.
    final_df = final_df.rename(columns={'short_desc': 'units'})
    
    # Define the 6 columns
    target_cols = [
        'state_name',           # 1. State
        'year',                 # 2. Year
        'commodity_desc',       # 3. Commodity (Column D)
        'units',                # 4. Units (Column J - Full Text)
        'statisticcat_desc',    # 5. Statistic (Column H)
        'Sales'                 # 6. Sales Value (Column AL - Cleaned)
    ]
    
    # Select only these columns (if they exist)
    final_cols = [c for c in target_cols if c in final_df.columns]
    final_df = final_df[final_cols]
    
    return final_df

# ==========================================
# 2. MAIN EXECUTION LOOP
# ==========================================
# Finds all CSV files in the folder with "_commodity_price" in the name
all_files = glob.glob("*_commodity_price*.csv") 
master_list = []

print(f"Found {len(all_files)} files. Starting processing...")

for f in all_files:
    print(f"  > Processing {f}...")
    df_clean = clean_nass_file(f)
    
    if not df_clean.empty:
        master_list.append(df_clean)
    else:
        print(f"    (No valid data found in {f})")

# ==========================================
# 3. SAVE OUTPUT
# ==========================================
if master_list:
    final_master_df = pd.concat(master_list, ignore_index=True)
    
    output_filename = "NASS_Commodity_Master_1975_2024.csv"
    final_master_df.to_csv(output_filename, index=False)
    
    print("-" * 50)
    print("PROCESSING COMPLETE")
    print(f"Total Rows Processed: {len(final_master_df)}")
    print(f"Columns: {list(final_master_df.columns)}")
    print(f"File Saved As: {output_filename}")
    print("-" * 50)
else:
    print("Error: No data was processed. Please check your CSV files.")