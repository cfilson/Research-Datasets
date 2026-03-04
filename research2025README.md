# USDA NASS Agricultural Research Datasets

Python data pipeline and cleaned datasets built to support undergraduate economics research on U.S. agricultural policy. This project takes raw USDA NASS (National Agricultural Statistics Service) commodity data, cleans and standardizes it, constructs a crosswalk between datasets, and outputs a panel dataset ready for regression analysis.

---

## Overview

USDA NASS publishes agricultural data across multiple sources (Census of Agriculture, Organic Census, Horticultural Census), each on different reporting schedules and with inconsistent formatting. This makes it difficult to use the data directly for time-series or panel regression work.

This project solves that by:

1. **Cleaning** raw NASS CSV exports (filtering by source, unit, and statistic type)
2. **Standardizing** the timeline from 1975 to 2024 with dynamic backfill logic
3. **Deduplicating** overlapping entries to prevent double counting
4. **Building a crosswalk** to link commodity categories across different USDA datasets
5. **Outputting** a single master panel dataset (87K+ rows, 50 states, 165 commodities) ready for Stata or Python regression analysis

---

## Data Pipeline (`nass_commodity.py`)

The Python script processes raw NASS CSV files through the following steps:

**Filtering:**
- Keeps only CENSUS source data (excludes SURVEY to avoid conflicting values within census blocks)
- Keeps only rows measured in dollars, discarding acres, head, and lbs
- Keeps only SALES or SALES DISTRIBUTION statistics

**Deduplication:**
- When both SALES and SALES DISTRIBUTION exist for the same state/year/commodity, the script prioritizes SALES to avoid double counting

**Timeline Expansion and Backfill:**
- Expands every commodity/state group to a full 1975-2024 timeline
- Uses dynamic backfill: each census value fills backward to (previous census year + 1)
- Handles different census cadences (every 5 years for Ag Census, every 3 years for Organic, etc.)
- First data point backfills up to 4 years max

**Data Cleaning:**
- Strips commas from numeric fields
- Converts NASS privacy codes like (D) and (Z) to NaN
- Flags potential subcategory commodities that could cause double counting with parent TOTALS

---

## Output Dataset

**File:** `NEW_NASS_Commodity_Master_1975_2024.csv`

| Column | Description |
|--------|-------------|
| `state_name` | U.S. state |
| `year` | Year (1975-2024) |
| `commodity_desc` | Commodity name (e.g., CORN, CATTLE, ALPACAS) |
| `units` | Full measurement description |
| `statisticcat_desc` | Statistic type (SALES or SALES DISTRIBUTION) |
| `Sales` | Cleaned numeric dollar value |
| `subcategory_flag` | True if commodity may be a subcategory (flagged for review) |

- **Total Rows:** 87,392
- **States:** All 50 U.S. states
- **Commodities:** 165
- **Time Span:** 1975 to 2024

---

## Crosswalk

The `crosswalk.csv` file maps commodity names across different USDA data sources (NASS commodity data, cash receipts, and export data) so they can be joined into a single panel dataset for regression analysis. This was necessary because the same commodity is often named differently across USDA databases.

---

## Additional Datasets

The repo also includes cleaned versions of:

- **Cash Receipts** data (in `/cash reciepts/`)
- **Export** data (in `/exports/`)

These were merged with the NASS commodity data using the crosswalk to build the final panel dataset used in the research.

---

## How to Run

**Requirements:**
```
pip install pandas numpy
```

**Steps:**
1. Place `nass_commodity.py` and your raw NASS CSV files in the same folder
2. Input files must have `_commodity_price` in the filename (e.g., `CA_commodity_price.csv`)
3. Run: `python nass_commodity.py`
4. Output will be saved as `NASS_Commodity_Master_1975_2024.csv`

---

## Tech Stack

- **Python** (Pandas, NumPy) for data processing and pipeline logic
- **Stata** for downstream regression analysis
- **USDA NASS** as the primary data source

---

## Files

| File | Description |
|------|-------------|
| `nass_commodity.py` | Main data cleaning and processing script |
| `NEW_NASS_Commodity_Master_1975_2024.csv` | Cleaned master dataset (87K+ rows) |
| `crosswalk.csv` | Commodity name mapping across USDA data sources |
| `/cash reciepts/` | Cleaned cash receipts data |
| `/exports/` | Cleaned export data |

---

## Context

This pipeline was built as part of an undergraduate research assistantship at UC Santa Cruz, working with Professor Galina Hale on agricultural policy research. The goal was to construct reliable panel datasets from messy federal data sources to support econometric analysis of U.S. agricultural trade and policy impacts.

---

## Author

**Cole Filson**
University of California, Santa Cruz | B.S. Technology Information Management & B.A. Economics (2026)
[LinkedIn](https://linkedin.com/in/colefilson) | colenf44@gmail.com
