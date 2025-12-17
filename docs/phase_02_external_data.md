# ♦︎ Phase 2 – External Data Integration (UNESCO Education & Literacy Data)


## Purpose

To compensate for the high missingness in World Bank education and literacy indicators, an additional dataset from the UNESCO Institute for Statistics was integrated into the project. The goal was to enrich the literacy and schooling dimension of the dashboard and ensure more complete time-series coverage across the full MENA region.

## Data Source

UNESCO Institute for Statistics (UIS) Downloaded as: UNESCO.csv

## Scope of UNESCO Indicators

The imported dataset includes key education and literacy metrics such as:

- Educational attainment rate, completed upper secondary education or higher, population 25+ years, female (%)
- Educational attainment rate, completed upper secondary education or higher, population 25+ years, male (%)
- Youth literacy rate, population 15-24 years, female (%)
- Youth literacy rate, population 15-24 years, male (%)
- Literacy rate, population 25-64 years, female (%)
- Literacy rate, population 25-64 years, male (%)

These metrics complement and extend the World Bank’s Education & Literacy category by filling structural gaps in the region.

## Processing Steps (Technical)

### Raw CSV Acquisition and Manual Pre-Filtering

The full dataset was downloaded directly from the UNESCO online portal.

Using Excel, the raw CSV was manually cleaned to retain only relevant fields: year, indicator_id, indicator_name, country_id, country_name, value

Indicators were mapped to their codes using VLOOKUP to ensure consistency with project metadata conventions.

Country names were normalized manually to match the World Bank naming scheme (e.g., “Egypt” → “Egypt, Arab Rep.”).

### Notebook Import and Standardization (notebooks/data_merge.ipynb)


Two dataframes were created for each dataset. Both were brought into a unified environment to ensure structural alignment. To ensure clean merging with no issues, both datasets were standardised. In addition, the UNESCO indicators were passed through the unit type inference system. This allowed UNESCO indicators to be seamlessly integrated into downstream EDA, modeling, and Tableau visualization.

### Data Merge Into Unified Long-Format Dataset

The two datasets were concatenated into a single long-format dataframe: combined = pd.concat([wb, unesco], ignore_index=True)

This merged dataset now contains: • Updated World Bank indicators (expanded to 18 MENA countries) • Supplementary UNESCO education/literacy indicators • Harmonized country names • Consistent indicator IDs and unit types

The final merged output was saved as: data/merged_data.csv

This file now serves as the new master input for the Phase 3 cleaning workflow.

