import pandas as pd 

# 1. READ THE FILE
# file_path: The name of your Excel file.
# sheet_name=None: Creates a dictionary of ALL tabs: {'Alabama': Data, 'Texas': Data...}
# header=2: Tells Python the actual column names start on Row 3 (Index 2).
print("Loading file")
all_sheets = pd.read_excel("CR_Annual (2).xlsx", sheet_name=None, header=2)

# Create an empty list to hold the clean dataframes
master_list = []

# 2. LOOP THROUGH EVERY TAB
# .items() lets us get the Tab Name (k) and the Data (v) at the same time
for state_name, df in all_sheets.items():
    
    # FILTER: Skip tabs that aren't states
    if state_name in ['Document Map', 'United States']:
        continue
        
    print(f"Processing: {state_name}...")

    # --- EXTRACT METADATA ---
    # The unit (e.g., "$1,000") is hidden in the first row of data.
    # We grab it from the column named '2024' (or first available year)
    # We use a trick: find columns that start with a digit (Years)
    year_cols = [c for c in df.columns if str(c).strip()[0].isdigit()]
    
    if not year_cols: continue # Safety check: if no years found, skip tab
        
    # Grab the unit from Row 0, First Year Column
    try:
        unit_val = df.iloc[0][year_cols[0]] 
    except:
        unit_val = "Unknown"

    # --- CLEAN THE TABLE ---
    # 1. Rename the first column (currently 'Alabama') to 'commodity'
    df.rename(columns={df.columns[0]: 'commodity'}, inplace=True)
    
    # 2. Delete junk rows
    # .iloc[1:] deletes the first row (which was just the unit info)
    df = df.iloc[1:] 
    # Keep only rows where 'commodity' has text (removes empty spacing rows)
    df = df[df['commodity'].notna()]

    # --- MELT (THE TRANSFORMATION) ---
    # Turns "Wide" data (years as columns) into "Long" data (rows)
    df_long = df.melt(
        id_vars=['commodity'],      # Keep this column fixed
        value_vars=year_cols,       # Unstack these specific year columns
        var_name='year',            # Name the new column 'year'
        value_name='Sales'          # Name the values 'Sales'
    )
    
    # --- ADD TAGS ---
    # Now that it's melted, we tag every row with the State and Unit
    df_long['State'] = state_name
    df_long['units'] = unit_val
    
    # Add this clean state dataframe to our master list
    master_list.append(df_long)

    # 3. CONCATENATE (STACK)
# ignore_index=True resets the row numbers so they go 0, 1, 2... 50000
final_df = pd.concat(master_list, ignore_index=True)

# 4. REORDER COLUMNS
# Put them in the logical order for a database
final_df = final_df[['State', 'year', 'commodity', 'units', 'Sales']]

# 5. DATA TYPE FIX (Optional but recommended)
# Ensure Sales are numbers, coercing errors (like "NA") to NaN
final_df['Sales'] = pd.to_numeric(final_df['Sales'], errors='coerce')

# Preview the result
print("\nFinal Data Preview:")
print(final_df.head())

# 6. SAVE
final_df.to_csv("USDA_Master_Dataset.csv", index=False)
print("Done! File saved.")