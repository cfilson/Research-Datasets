import pandas as pd

def clean_exports_final(file_path):
    print(f"Loading {file_path}...")
    
    # 1. READ ALL SHEETS (No header initially to inspect structure)
    try:
        all_sheets = pd.read_excel(file_path, sheet_name=None, header=None)
    except Exception as e:
        print(f"Error loading file: {e}")
        return pd.DataFrame()

    master_list = []
    
    # 2. ITERATE THROUGH TABS (Commodities)
    for commodity_name, df in all_sheets.items():
        # Skip empty sheets
        if df.empty:
            continue
            
        # --- A. FIND THE DATA START ---
        # Look for the row where the first cell is "Alabama"
        data_start_row = -1
        for i in range(min(20, len(df))):
            val = str(df.iloc[i, 0]).strip()
            if val == "Alabama":
                data_start_row = i
                break
        
        if data_start_row == -1:
            # If Alabama isn't found, this might be a map or summary tab
            continue

        # --- B. SURGICAL EXTRACTION ---
        # We need 26 columns: Column 0 (State) + Columns 1-25 (Years 2000-2024)
        if df.shape[1] < 26:
             continue

        # Slice from the "Alabama" row down to the end
        df_subset = df.iloc[data_start_row:, 0:26].copy()
        
        # --- C. RENAME COLUMNS MANUALLY ---
        # Hardcode names: State, 2000, 2001... 2024
        new_columns = ['State'] + [str(year) for year in range(2000, 2025)]
        df_subset.columns = new_columns
        
        # --- D. CLEAN UP ROWS ---
        # Drop empty states
        df_subset = df_subset[df_subset['State'].notna()]
        
        # Filter out junk rows (Totals, Ranks, Footnotes)
        junk_list = ["United States", "Total", "Rank", "Million dollars", "Source", "Footnotes"]
        pattern = '|'.join(junk_list)
        df_subset = df_subset[~df_subset['State'].astype(str).str.contains(pattern, case=False, na=False)]

        # --- E. MELT TO LONG FORMAT ---
        year_cols = [str(y) for y in range(2000, 2025)]
        
        df_long = df_subset.melt(
            id_vars=['State'],
            value_vars=year_cols,
            var_name='year',
            value_name='Exports'
        )
        
        # --- F. ADD METADATA ---
        df_long['commodity'] = commodity_name
        df_long['units'] = "Million dollars"
        
        master_list.append(df_long)

    # 3. COMBINE & EXPORT
    if master_list:
        final_df = pd.concat(master_list, ignore_index=True)
        # Final Order: State, year, commodity, units, Exports
        final_df = final_df[['State', 'year', 'commodity', 'units', 'Exports']]
        return final_df
    else:
        return pd.DataFrame()

# --- RUN IT ---
file_name = "commodity-detail-by-state-cy (1).xlsx"
df_result = clean_exports_final(file_name)

if not df_result.empty:
    df_result.to_csv("Cleaned_Export_Data_Final.csv", index=False)
    print("Success! Data saved to 'Cleaned_Export_Data_Final.csv'")
    print(df_result.head())
else:
    print("Failed to extract data. Please check file name.")


#final
