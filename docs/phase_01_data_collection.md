# ♦︎ Phase 1 – API Fetching & Data Cleaning (Python)

This phase covers the automated extraction, refinement, and preprocessing of gender, education, demographic, and economic indicators from the World Bank API and supplementary UNESCO data. As the analytical direction of the project became clearer, the indicator list, country coverage, and data scope were expanded to support a richer and more complete narrative across the MENA region.

## Indicator Refinement and Expansion

Several indicators originally included were removed due to limited analytical value or static, non-informative values. Examples include HIV prevalence indicators and certain labor-force metrics that showed little to no variation across countries and years. In their place, more relevant and story-aligned indicators were added, including:

• Full Women, Business & the Law (WBL) component indices • Human Capital Index (HCI) indicators • Gini coefficient • Population and demographic measures • Employment-to-population ratios (replacing weaker labor-force metrics)

Country coverage was expanded from 13 to 18 countries to fully represent the MENA region. All new indicators and countries were incorporated into the API-fetching module, and an updated master dataset was generated reflecting this expanded scope.

## 🌐 Countries Covered

Algeria, Bahrain, Egypt, Iraq, Jordan, Kuwait, Lebanon, Libya, Morocco, Oman, Qatar, Saudi Arabia, Syria, Tunisia, United Arab Emirates, Yemen, Palestine, Iran

## 📊 Indicators


The dataset now includes 45+ gender, social, economic, legal, and demographic indicators such as literacy rates, WBL Index scores, HCI components, Gini inequality measures, employment ratios, and life expectancy. All indicators include standardised metadata fields: Indicator Code, Indicator Name, Country, Year, and Value.

## ⦿ Step 1: API Fetching (src/api_fetcher.py)


#### Purpose

Automates the fetching of World Bank data and exports a unified dataset for all selected indicators and countries.

#### Main Features


- Uses requests with robust retry logic for API stability.
    
- Handles pagination to fetch all pages per indicator.
    
- Fetches all 13 countries simultaneously for speed.
    
- Saves:
    
    - IND_.csv – per-indicator raw exports
    - _selected.csv – per-country filtered exports
    - all_countries_selected.csv – combined dataset for cleaning
- Automatically applies unit type inference using src/unit_types.py.
    

#### Output Example


```shell
data/
├── IND_SG.LAW.INDX.csv
├── EGY_selected.csv
├── all_countries_selected.csv
logs/
└── fetch.log
```

## ⦿ Step 2: Unit Type Inference (src/unit_types.py)


#### Purpose

Adds a clean, standardized unit_type label to every indicator.


#### Logic

Uses rule-based text detection to classify units such as:

- Percent (%)
    
- Index / Score
    
- Binary (1=yes; 0=no)
    
- Currency (US$)
    
- Rate per 1,000 or 100,000
    
- Ratio or Parity (GPI)
    
- Count / Population
    

#### Example

| Indicator Name                         | Unit Type      |
| -------------------------------------- | -------------- |
| Women Business and the Law Index Score | Index / Score  |
| Labor force participation rate, female | Percent (%)    |
| Birth rate, crude (per 1,000 people)   | Rate per 1,000 |
