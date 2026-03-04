import pandas as pd
import numpy as np
import glob
import os

# 
# 1. ENVIRONMENT SETUP
#
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
    1. Filter: Keeps only CENSUS source data (excludes SURVEY).
    2. Filter: Keeps only rows measured in Dollars ($).
    3. Filter: Keeps only 'SALES' or 'SALES DISTRIBUTION' statistics.
    4. Deduplication: If both exist, prioritizes 'SALES' to avoid double counting.
    5. Time Series: Expands timeline to 1975-2024.
    6. Imputation: Dynamic backfill - each census value fills back to previous census + 1.
    7. Flag: Marks potential subcategory commodities for review.
    """
    try:
       
        df = pd.read_csv(file_path, low_memory=False)
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()
    
    # 
    # STEP 1: filter to Census source only (exclude Survey data)
    # 
    # NASS data contains both CENSUS and SURVEY sources.
    # Survey data is published annually and creates multiple different
    # values within a single census block during backfill.
    if 'source_desc' in df.columns:
        df = df[df['source_desc'] == 'CENSUS']

    # 
    # STEP 2: filter by unit 
    # 
    # strictly require 'unit_desc' to be '$'. 
    # Any row measuring Head, Acres, or Lbs is discarded.
    if 'unit_desc' in df.columns:
        df = df[df['unit_desc'] == '$']
    else:
        return pd.DataFrame() # Skip file if standard columns are missing
    
    # 
    # STEP 3: filter by stat (Column H)
    # 
    # only want Sales figures.
    target_stats = ['SALES', 'SALES DISTRIBUTION']
    if 'statisticcat_desc' in df.columns:
        df = df[df['statisticcat_desc'].isin(target_stats)]
        
        # 
        # STEP 4: handle double counting 
        # 
        # Some commodities list both "SALES" and "SALES DISTRIBUTION".
        # prefer "SALES". Sorting Ascending places 'SALES' before 'SALES DISTRIBUTION'.
        # keep the 'first' occurrence for each State/Year/Commodity.
        df = df.sort_values('statisticcat_desc', ascending=True)
        
        cols_for_dupe = ['state_name', 'year', 'commodity_desc']
        if all(c in df.columns for c in cols_for_dupe):
            df = df.drop_duplicates(subset=cols_for_dupe, keep='first')

    # 
    # STEP 5: clean sales value (Column AL)
    # 
    # Raw NASS data contains commas ("1,000") and privacy codes like "(D)" or "(Z)".
    # - remove commas.
    # - force data to numeric, turning "(D)" and "(Z)" into NaN (Blank).
    if 'Value' in df.columns:
        df['Sales'] = df['Value'].astype(str).str.replace(',', '', regex=False)
        df['Sales'] = pd.to_numeric(df['Sales'], errors='coerce')
    else:
        df['Sales'] = np.nan

    # 
    # STEP 6: dynamic backfill (1975-2024)
    # 
    # NASS Census data can appear on different cadences:
    #   - Census of Agriculture: every 5 years (2002, 2007, 2012, 2017, 2022)
    #   - Organic Census: roughly every 3 years (2008, 2011, 2014, 2019)
    #   - Horticultural Census: its own schedule (2009, 2014, 2019)
    #
    # Each census value fills backward to (previous census year + 1).
    # This keeps blocks constant regardless of cadence.
    # The first data point backfills up to 4 years max.
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
        
        # --- DYNAMIC BACKFILL ---
        # Find years that have actual census data
        data_years = sorted(merged.dropna(subset=['Sales'])['year'].tolist())
        
        if not data_years:
            continue
        
        # For each census data point, fill backward to (previous data year + 1)
        for i, yr in enumerate(data_years):
            if i == 0:
                # First data point: backfill up to 4 years
                start = max(yr - 4, 1975)
            else:
                # Fill back to the year after the previous census
                start = data_years[i - 1] + 1
            
            val = merged.loc[merged['year'] == yr, 'Sales'].iloc[0]
            mask = (merged['year'] >= start) & (merged['year'] <= yr)
            merged.loc[mask, 'Sales'] = val
        
        # Remove rows that are still blank (no census data was close enough)
        merged = merged.dropna(subset=['Sales'])
        
        # Ensure we restrict the final output to exactly 1975-2024
        merged = merged[(merged['year'] >= 1975) & (merged['year'] <= 2024)]
        
        processed_chunks.append(merged)
        
    if not processed_chunks:
        return pd.DataFrame()
        
    final_df = pd.concat(processed_chunks, ignore_index=True)
    
    # 
    # STEP 7: final formatting
    # 
    # Rename Column J ('short_desc') to 'units'.
    final_df = final_df.rename(columns={'short_desc': 'units'})
    
    # Flag potential subcategory commodities for review.
    # Subcategories have a comma in the name (e.g., "BEDDING PLANTS, ANNUAL")
    # but are not aggregate totals (e.g., "BEDDING PLANT TOTALS").
    # These may cause double counting with their parent TOTALS category.
    final_df['subcategory_flag'] = (
        final_df['commodity_desc'].str.contains(',') & 
        ~final_df['commodity_desc'].str.contains('TOTAL')
    )
    
    # Define the 7 columns
    target_cols = [
        'state_name',           # 1. State
        'year',                 # 2. Year
        'commodity_desc',       # 3. Commodity (Column D)
        'units',                # 4. Units (Column J - Full Text)
        'statisticcat_desc',    # 5. Statistic (Column H)
        'Sales',                # 6. Sales Value (Column AL - Cleaned)
        'subcategory_flag'      # 7. Flag for potential subcategories
    ]
    
    # Select only these columns (if they exist)
    final_cols = [c for c in target_cols if c in final_df.columns]
    final_df = final_df[final_cols]
    
    return final_df

#
# 2. execution loop 
# 
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


# 3. save output 

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