# 🌐 MENA Gender Data Dashboard 
#### End-to-End Data Pipeline Overview

## ⦿ Project Purpose
The MENA Gender Data Dashboard is a data-driven platform that tracks gender-related development indicators across 13 Middle East and North Africa countries.
It integrates automated data collection, cleaning, statistical analysis, machine learning, and visualisation into a single reproducible pipeline.
The goal is to transform fragmented World Bank Gender Statistics into clear, actionable insights for researchers and policy analysts.



## ⦿ High-Level Architecture

```mermaid
flowchart LR
    %% Direction: Left → Right
    %% --- GLOBAL STYLING ---
    classDef phase fill:#222833,stroke:#999,stroke-width:2px,color:#ffffff,font-size:20px,font-weight:bold,padding:15px;
    classDef sub fill:#2c2f38,stroke:#777,stroke-width:1px,color:#f0f0f0,font-size:16px,font-weight:500,padding:10px;
    classDef flow stroke:#aaa,stroke-width:1px;

    %% --- MAIN PHASES ---
    A([Phase 1: Data Ingestion]):::phase
    B([Phase 2: Data Cleaning & Validation]):::phase
    C([Phase 3: SQL Integration]):::phase
    D([Phase 4: Analytics & ML]):::phase
    E([Phase 5: Visualization & Insights]):::phase

    %% --- PIPELINE FLOW ---
    A -->|Collect & Standardize| B
    B -->|Validate & Prepare| C
    C -->|Integrate Schema| D
    D -->|Model & Explain| E

    %% --- SUBPHASES ---
    subgraph A1[ ]
        direction TB
        A1a[World Bank API Fetcher]:::sub
        A1b[Unit Type Inference]:::sub
    end

    subgraph B1[ ]
        direction TB
        B1a[Duplicates & Null Handling]:::sub
        B1b[Contextual Filling Rules]:::sub
        B1c[Structural Gap Flagging]:::sub
    end

    subgraph C1[ ]
        direction TB
        C1a[Schema Design & Keys]:::sub
        C1b[Country & Indicator Joins]:::sub
        C1c[Stored Views for Queries]:::sub
    end

    subgraph D1[ ]
        direction TB
        D1a[Clustering & Anomaly Detection]:::sub
        D1b[Classification of High-Risk Countries]:::sub
        D1c[Explainable AI – SHAP + LIME]:::sub
    end

    subgraph E1[ ]
        direction TB
        E1a[Tableau Interactive Dashboard]:::sub
        E1b[Policy Insight Narratives]:::sub
    end

    %% --- ALIGN SUBGRAPHS ---
    A1 --- B1 --- C1 --- D1 --- E1


```
## ⦿ Pipeline Summary by Phase


| Phase                                 | Description                                                                                                | Key Tools / Outputs                                              |
| ------------------------------------- | ---------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| **1 – API Fetching & Pre-processing** | Retrieve 45 gender, education, and economic indicators for 13 MENA countries (2000–2024). Add unit types.  | `Python · requests · pandas` → `data/all_countries_selected.csv` |
| **2 – Data Cleaning & Validation**    | Remove duplicates, handle nulls, apply contextual fills, flag structural missing years.                    | Cleaned CSV (< 10 % nulls)                                       |
| **3 – SQL Integration**               | Import clean dataset into MySQL schema; normalize and create views for queries.                 | `SQL DDL/DML scripts · ERD`                                      |
| **4 – Analytics & ML Models**         | Clustering (K-Means), Anomaly Detection, Classification (Random Forest/SVM), Explainability (SHAP).        | `MLflow · scikit-learn · Pandas`                                 |
| **5 – Visualization & Insights**      | Interactive dashboard with time-series and geospatial visuals; gender-gap heatmaps and policy annotations. | `Tableau · Python plots`                              |


## ⦿ Key Deliverables

- Automated Data Fetcher: 13-country pipeline with retry logic and pagination

- Clean Unified Dataset: 2000–2023 data across 45 indicators

- SQL Schema + Queries: reusable relational model for analysis

- Machine Learning Modules: clustering + risk prediction

- Interactive Dashboard: gender gap visuals and policy context

_________________


# ♦︎ Phase 1 – API Fetching & Data Cleaning (Python)


This phase covers the automated extraction, refinement, and preprocessing of gender, education, demographic, and economic indicators from the World Bank API and supplementary UNESCO data. As the analytical direction of the project became clearer, the indicator list, country coverage, and data scope were expanded to support a richer and more complete narrative across the MENA region.

## Indicator Refinement and Expansion

Several indicators originally included were removed due to limited analytical value or static, non-informative values. Examples include HIV prevalence indicators and certain labor-force metrics that showed little to no variation across countries and years. In their place, more relevant and story-aligned indicators were added, including:

• Full Women, Business & the Law (WBL) component indices
• Human Capital Index (HCI) indicators
• Gini coefficient
• Population and demographic measures
• Employment-to-population ratios (replacing weaker labor-force metrics)

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
  - IND_<indicator>.csv – per-indicator raw exports
  - <country>_selected.csv – per-country filtered exports
  - all_countries_selected.csv – combined dataset for cleaning

- Automatically applies unit type inference using src/unit_types.py.

#### Output Example  

```bash
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


# ♦︎ Phase 2 –  External Data Integration (UNESCO Education & Literacy Data)

## Purpose

To compensate for the high missingness in World Bank education and literacy indicators, an additional dataset from the UNESCO Institute for Statistics was integrated into the project. The goal was to enrich the literacy and schooling dimension of the dashboard and ensure more complete time-series coverage across the full MENA region.

## Data Source

UNESCO Institute for Statistics (UIS)
Downloaded as: UNESCO.csv

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

Using Excel, the raw CSV was manually cleaned to retain only relevant fields:
year, indicator_id, indicator_name, country_id, country_name, value

Indicators were mapped to their codes using VLOOKUP to ensure consistency with project metadata conventions.

Country names were normalized manually to match the World Bank naming scheme (e.g., “Egypt” → “Egypt, Arab Rep.”).

### Notebook Import and Standardization (notebooks/data_merge.ipynb)
Two dataframes were created for each dataset. Both were brought into a unified environment to ensure structural alignment. To ensure clean merging with no issues, both datasets were standardised. In addition, the UNESCO indicators were passed through the unit type inference system. This allowed UNESCO indicators to be seamlessly integrated into downstream EDA, modeling, and Tableau visualization.

### Data Merge Into Unified Long-Format Dataset

The two datasets were concatenated into a single long-format dataframe:
combined = pd.concat([wb, unesco], ignore_index=True)

This merged dataset now contains:
• Updated World Bank indicators (expanded to 18 MENA countries)
• Supplementary UNESCO education/literacy indicators
• Harmonized country names
• Consistent indicator IDs and unit types

The final merged output was saved as:
data/merged_data.csv

This file now serves as the new master input for the Phase 3 cleaning workflow.


# ♦︎ Phase 3: Data Cleaning & Validation

#### Location: notebooks/cleaning.ipynb

#### Output files:
- ml_df.csv → cleaned + interpolated dataset for ML
- final_df_for_tableau.csv → cleaned + enriched + raw-kept dataset for Tableau storytelling

#### Purpose

The goal of Phase 3 was to transform the raw merged dataset into two things:

1. A clean, analysis-ready dataset for ML modeling (minimal missingness).

2. A comprehensive Tableau dataset that preserves even structurally missing values to visualize real-world data gaps in gender & education indicators across the MENA region.

This phase focused heavily on handling complex missingness patterns arising from two different sources (World Bank + UNESCO).


#### Step-by-Step Cleaning Workflow
##### 1. Fix and Standardize Raw Fields.
   
✔ Converted value column to numeric using:
```bash
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
```

✔ Confirmed expected columns:

```bash
country_name | year | indicator_id | indicator_name | value | unit_type | source | source_reliable
```

##### 2.Identify Completely Empty Indicators (Structural Missingness)

##### 3. Deep-dive into Education (UNESCO + WHO) Data to Assess Coverage.

Since education & literacy were known to be sparse:
a) Created a source flag (WHO vs UNESCO)

Checked which indicators came from which source:

```bash
    df['source_reliable'] = df['indicator_name'].apply(
    lambda x: "low" if indicator_class[x] == "low missingness" else "high"
)
```

b) Computed coverage for each source separately

- who_only

- unesco_only

- both_sources (none found)

- none_sources (3715 rows with missing from both)

Then:

```bash
who_coverage = who_only["value"].isna().mean()
unesco_coverage = unesco_only["value"].isna().mean()
```

Result:

- WHO avg missingness ≈ 17%

- UNESCO avg missingness ≈ 14% (but extremely patchy by country)

c) Country-level coverage patterns (major insight)

| Country          | % Missing |
| ---------------- | --------- |
| Libya            | **100%**  |
| Yemen            | 92%       |
| Saudi Arabia     | 100%      |
| UAE              | 92%       |
| Bahrain          | 81%       |
| Egypt            | 68%       |
| Morocco          | 88%       |
| Tunisia          | 50%       |
| Iran             | 36%       |
| West Bank & Gaza | 18%       |

➡ Conclusion:
Education data is severely limited, especially for GCC + conflict-affected countries.
➡ Therefore: UNESCO / WHO education indicators cannot be used for ML but are extremely valuable for the dashboard narrative (data deserts).




##### 4. Classify Missingness Into 4 Categories:

```bash
def classify(m):
    if m > 0.75: 
        return "severely incomplete"
    elif m > 0.45:
        return "high missingness"
    elif m > 0.20:
        return "moderate missingness"
    else:
        return "low missingness"
```

Example indicator missingness:
| Indicator                                   | Missing % | Category                |
| ------------------------------------------- | --------- | ----------------------- |
| Contraceptive prevalence (modern)           | 0.87      | **Severely incomplete** |
| Gini Index                                  | 0.86      | **Severely incomplete** |
| School enrollment GPI                       | 0.45–0.60 | High/moderate           |
| HCI (all versions)                          | 0.38–0.70 | Moderate/High           |
| Fertility, Life expectancy, Employment, GDP | <5%       | Low                     |



##### 5. Rule-Based Missing Value Treatment (Your Final Framework)

| **Missingness Class**   | **% Null** | **Rule Applied**              | **Why**                                               |
| ----------------------- | ---------- | ----------------------------- | ----------------------------------------------------- |
| **Low**                 | <20%       | `interpolate_full`            | Smooth, stable indicators → safe to interpolate fully |
| **Moderate**            | 20–45%     | `interpolate_safe` (limit=3)  | Fill short gaps only                                  |
| **High**                | 45–75%     | `interpolate_short` (limit=1) | Avoid overfitting trends                              |
| **Severely Incomplete** | >75%       | **keep_raw**                  | Too sparse → used only to visualize data gaps         |



##### 6. Apply Cleaning Rules Per Indicator × Country

```bash
for ind, group in tqdm(clean_df.groupby("indicator_name")):
    rule = CLEANING_RULES.get(indicator_class[ind], "keep_raw")
    cleaned = (
        group[["country_name","year","value"]]
        .sort_values("year")
        .groupby("country_name", group_keys=False)
        .apply(CLEANING_FUNCS[rule])
        .reset_index(drop=True)
    )

    cleaned_full = cleaned.merge(
        group.drop(columns=["value"]),
        on=["country_name","year"],
        how="left"
    )
```


##### 7.Removed year 2024 (structurally too incomplete)

##### Final Deliverables:

A. ML-Ready Dataset (ml_df)

✔ Missing values reduced to 282
✔ Fully cleaned + interpolated
✔ Ready for feature engineering + ML modeling (clustering, classification, PCA, SHAP)

B. Tableau Dataset (final_df_for_tableau)

Merged cleaned WB + cleaned UNESCO + raw UNESCO (for missingness transparency)

This dataset:

- Keeps all indicators, even those with 100% missing

- Includes a source and source_reliable tag

- Preserves missingness for your data desert story

- Perfect for Tableau visuals



# ♦︎ Phase 4 – Feature Engineering Preparation (ML Dataset Construction)

After completing the full data-cleaning workflow, an additional preparation stage was carried out to ensure the dataset was fully ready for machine-learning analysis. This involved semantic enrichment, feature reduction, and final null-resolution, transforming the cleaned long-format dataset into a robust ML-ready resource.

This phase produced ml_df, a dataset with 0 missing values, consistent time-series formatting, and enhanced contextual metadata.

## 1. Semantic Enrichment: Adding Region & Category Columns

To support downstream modeling, two high-level contextual variables were added:

### 1.1 Region Mapping

Each country was assigned to one of the following regions:

- GCC

- Levant

- North Africa

Other (Iran, Yemen, Iraq)
###### Note:
Iraq, Iran and Yemen were classified as “Other” because they do not fully align with the political or geographic boundaries of the GCC, Levant, or North African subregions.

##### Why this matters:
Many indicators show strong regional clustering (e.g., political representation, literacy, economic indicators). Region labels allow:

- region-level aggregations

- improved imputation

- more meaningful feature engineering

- potential geo-sensitive ML performance

  
### 1.2 Indicator Category Mapping

Every indicator was assigned to a broader category (Education, Health, Economic Participation, Legal Rights, Demographics, etc.).

##### Why this matters:

- Helps group indicators logically during feature engineering

- Improves interpretability in ML models

- Allows category-specific transformations (e.g., volatility expectations differ for economic vs. legal indicators)


## 2. Feature Reduction: Dropping Structurally Incomplete Indicators

Certain indicators—especially the Human Capital Index (HCI) series—contained extremely high missingness (80–100%) across nearly all countries. These were not missing at random but due to structural non-reporting.


Why HCI was removed:

- Published only from 2018 onward

- Many conflict states have 0 valid observations

- Long missing blocks prevent meaningful interpolation

- Would add noise, not information, to ML models

➡️ HCI indicators were removed from the ML dataset but retained for the dashboard to illustrate coverage gaps.

This reduction significantly improved the consistency and usability of the ML dataset.


## 3. Null Resolution in Two Stages

After filtering structurally unusable indicators, the dataset still contained a small number of nulls. These were resolved using a two-phase targeted imputation strategy.

### Phase 1 — Time-Series Interpolation

Indicators were grouped by indicator_name, country_name, and year, and interpolated according to their missingness class:

- Moderate → short-gap interpolation

- Small Gap → minimal interpolation (ffill/bfill only)

This reduced nulls from 166 → 105.


### Phase 2 — Regional Mean Imputation

The remaining missing values belonged to a small set of indicators where:

- interpolation was mathematically inappropriate

- missingness followed regional patterns

- regional socio-economic similarity justified region-based imputation

Imputation logic:

1. Compute regional year-wise mean for the indicator

2. Fill missing values using this regional anchor

3. Apply nearest-year fallback only when strictly needed

This is statistically safer than global means and preserves geopolitical structure.

Result:
➡️ All remaining nulls were resolved (0 missing values total).


## Summary of Phase 4 Improvements

| Step                                | Purpose                       | Impact                                           |
| ----------------------------------- | ----------------------------- | ------------------------------------------------ |
| Added region & category metadata    | Enhance context               | Better feature engineering & ML interpretability |
| Removed incomplete indicators (HCI) | Prevent noise & bias          | Clean, reliable ML dataset                       |
| Time-series interpolation           | Repair natural gaps           | Smooth and realistic trends                      |
| Regional mean imputation            | Fix non-interpolatable values | 100% completeness without distortion             |
| Validation                          | Ensure readiness              | `ml_df` fully prepared for ML modeling           |


# ♦︎ Phase 5 – Feature Engineering 

The goal was to convert raw indicator values into a set of temporal, structural, and contextual features that allow machine learning models to detect patterns, predict risk, and classify countries based on gender-related development behavior.

### 1. Temporal Dynamics

| Feature           | Purpose                      | ML Value                                               |
| ----------------- | ---------------------------- | ------------------------------------------------------ |
| **YoY Change**    | Captures short-term movement | Early detection of policy shifts and emerging patterns |
| **YoY % Change**  | Normalizes rate of change    | Enables ML to compare indicators with different scales |
| **Rolling Means** | Smooth long-term signal      | Improves model stability and reduces noise             |
| **Rolling STD**   | Measures volatility          | Predicts instability and susceptibility to shocks      |
| **CAGR**          | Multi-year growth rate       | Summarizes long-term direction for classifiers         |


### 2. Shock & Event Features

| Feature      | Meaning                   | ML Role                                            |
| ------------ | ------------------------- | -------------------------------------------------- |
| **Spikes**   | Sudden increases (> 2 SD) | Detects policy breakthroughs or sharp improvements |
| **Dips**     | Sudden drops (< -2 SD)    | Detects crisis years (war, unrest, recession)      |
| **Outliers** | Extreme values (IQR)      | Enhances robustness by controlling for noise       |

These features are crucial for studying how gender indicators respond to political, economic, or social shocks across the MENA region.


### 3. Trend & Stability Features

| Feature                      | Description                                 | ML Contribution                                    |
| ---------------------------- | ------------------------------------------- | -------------------------------------------------- |
| **Trend Slope**              | Linear regression slope of indicator values | Captures long-term direction (improving/declining) |
| **Coefficient of Variation** | Normalized volatility                       | Differentiates stable vs. unstable countries       |
| **Momentum**                 | Acceleration of change                      | Detects turning points, nonlinear transitions      |


### 4. Contextual Features

| Feature                | Purpose                        | ML Role                                              |
| ---------------------- | ------------------------------ | ---------------------------------------------------- |
| **Regional Deviation** | Distance from regional average | Controls for shared shocks (Arab Spring, oil crisis) |

By adding region and category columns before feature creation, each feature is embedded in geopolitical and thematic context.


### Conclusion

The engineered dataset now contains rich temporal signatures, event reaction patterns, and context-sensitive indicators, making it suitable for:

- Clustering countries by developmental trajectories

- Classification of high-risk countries

- Anomaly detection for crisis years

- Dimensionality reduction (PCA/UMAP) for pattern visualization

- Explainable AI (SHAP, LIME) to interpret drivers of gender inequality



# ♦︎ Phase 6 – ML Prep and Principal Component Analysis 

Location: notebooks/mL_prep.ipynb

## Purpose

This phase prepared the cleaned dataset for machine-learning analysis by (1) selecting a statistically sound feature subset using a correlation matrix, and (2) applying PCA to uncover the core latent dimensions that drive gender-development differences across MENA countries.

## 1. Feature Selection Using Correlation Matrix

The initial dataset contained 60+ engineered indicators. Before modeling, we performed feature reduction to prevent multicollinearity, over-weighting, and the curse of dimensionality.

#### Process

- Computed a full correlation matrix on high-variance indicators

- Identified tight clusters (GDP, population, WBL sub-indices, ministerial/parliament indicators)

- Removed redundant indicators showing correlation > 0.90

- Retained only structurally unique, high-signal features


#### Final Feature Set
Nine indicators capturing the core gender-development dimensions:

1. Female labor force participation (%)

2. GDP per capita (US$)

3. Women in Parliament (%)

4. WBL Index Score

5. Education GPI (secondary)

6. Maternal mortality ratio

7. Adolescent fertility rate

8. Population (total)
   
9. Inflation
    

Why this matters:
This reduced set avoids dominance of any single thematic block and preserves interpretability for PCA and clustering.


## 2. Scaling & Standardization

All features were standardized (z-score) to remove unit differences (%, per 100k, index values, USD).
Standardization ensures that PCA treats each indicator equally.


## 3. PCA Fitting & Variance Reduction

A full PCA model was fitted on the scaled feature matrix.

Variance Structure

- PC1: 36.2%

- PC2: 18.6%

- PC3: 14.1%
  
→ Total explained by PC1–PC3: ~69%

The scree plot showed a strong elbow, confirming meaningful underlying structure.
<img width="1380" height="480" alt="image" src="https://github.com/user-attachments/assets/7cc1a9dd-747a-4f45-9b53-66b0cd41ca92" />

## 4. PCA Component Interpretation

#### PC1 — Structural Gender Development

Captures a development gradient driven by:

- GDP per capita, + female labor participation, − adolescent fertility, − maternal mortality.
  → Represents economic empowerment & health advancement.

#### PC2 — Legal Equality vs Political Representation

- High: WBL score, education parity
- Low: women in parliament
  → Represents institutional reforms vs actual political voice.

#### PC3 — Economic Instability & Uneven Gender Outcomes

- Mixed signals involving inflation, education, WBL, and health indicators.
  → Represents policy–outcome inconsistency under economic pressure.


## 5. PCA Country Distribution

The PC1 × PC2 scatter shows distinct regional patterns:

- GCC: high PC1 (strong development/economic empowerment)

- North Africa: mid-PC1, high-PC2 (strong laws/education vs weaker representation)

- Fragile States: low PC1 (health + demographic burden)
  
- Lebanon: PC2 outlier (high legal/education indicators but weak political structures)


# ♦︎ Phase 7 – PCA-Based Clustering Analysis

Location: notebooks/machine_L.ipynb

## Purpose

This phase used the PCA-transformed feature space to uncover structural groupings among MENA countries. By combining dimensionality reduction with clustering algorithms, the goal was to identify shared gender-development patterns and build country archetypes that can later be used for storytelling, dashboard segmentation, and policy interpretation.

## 1. Preparing PCA Output for Clustering

After PCA reduced the dataset to three core components (PC1–PC3), these components were used as the feature space for clustering.

Steps performed:

- Extracted PC1, PC2, and PC3 scores for each country

- Merged PCA output with country and region labels

- Ensured no missing values, consistent indexing, and proper scaling

- Confirmed that PCA space captures ~69% of total variance → sufficient for clustering structure

### Why PCA space?
It removes noise, minimizes multicollinearity, and ensures that clustering is driven by meaningful latent dimensions, not raw indicator scale differences.

## 2. Determining Optimal Number of Clusters

Multiple validation metrics were computed across k = 2 → 8:

- Elbow (Inertia)

- Silhouette Score

- Davies–Bouldin Index

- Calinski–Harabasz Index

Across these metrics, k = 5 consistently provided the best balance between cohesion and separation.

Silhouette ≈ 0.39
→ moderate separation, acceptable given the small dataset and strong regional heterogeneity.

A combined elbow–silhouette plot was saved as documentation.


## 3. Fitting K-Means on PCA Components

A K-Means model with k = 5 was fitted using PC1–PC3 as inputs.

Core logic:

- Countries positioned close in PCA space were grouped based on shared gender, economic, and demographic characteristics

- Cluster labels were added back into the ML dataset for profiling

- Cluster sizes were examined to ensure stability

This produced five interpretable archetypes.


## 4. Cluster Membership (Final Assignments)

The model yielded the following groupings:

| Cluster | Countries                                       | Interpretation                                                                               |
| ------- | ----------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **0**   | Lebanon                                         | Unique parity + inflation instability profile                                                |
| **1**   | Bahrain, Kuwait, Oman, Qatar, Saudi Arabia, UAE | High-income Gulf economies with strong economic empowerment but low political representation |
| **2**   | Egypt, Iran, Iraq, Jordan, Syria, Yemen         | High demographic pressure, high maternal mortality, low economic empowerment                 |
| **3**   | Algeria, Libya, Morocco, Tunisia                | North African reformist bloc with stronger laws and moderate outcomes                        |
| **4**   | West Bank & Gaza                                | An institutional/political outlier with atypically high representation metrics               |


## 5. Cluster Profile Construction

To interpret the clusters, we calculated the mean value of each selected feature within each group.

Indicators included:

- GDP per capita
- Women in parliament (%)
- WBL Index
- Education GPI
- Maternal mortality
- Adolescent fertility
- Female employment rate
- Inflation
- Population

Because raw values differ drastically in scale, all features were normalized (z-scores) before visualization.


## 6. Normalized Radar Charts (Cluster Personality Maps)

For each cluster, a radar chart was generated showing its standardized feature profile.

These charts reveal each group’s strengths, weaknesses, and structural signatures, such as:

- Cluster 1 (GCC): high GDP, high female employment, low adolescent fertility, low political representation

- Cluster 3 (North Africa): high WBL and education parity, moderate political voice, mixed economic strength

- Cluster 2 (Fragile States): extreme demographic pressure and health burden, low empowerment

- Cluster 0 (Lebanon): strong institutional/education indicators contrasted with economic instability

- Cluster 4 (West Bank): exceptional political representation relative to economic baseline

These profiles will feed directly into Tableau dashboards and thesis narrative sections.


# ♦︎ Phase 8 – Trajectory-Based Country Clustering

Location: notebooks/traj_clustering.ipynb
Inputs: feature-engineered panel from feature_eng.ipynb (country–year–indicator), plus new category-level summaries

## 1. Purpose & Link to the Main Storyline

The earlier structural PCA + clustering phase answered:

##### “Which countries look similar today in terms of their overall gender-development levels?”

This new phase moves from levels to movement:

##### “Which countries move similarly over time across gender, health, economic and legal dimensions?”
##### “Who improves steadily, who reforms in bursts, and who regresses or collapses under shocks?”

This directly serves:

1. Sub-storyline A – Long-term trajectories (slope, CAGR, momentum)

2. Sub-storyline B – Instability vs. resilience (volatility, spikes, dips)

3. Refines Main ML Goal 1: identifying country trajectory types.

All modelling in this phase is done at country level, using category-aggregated trajectory features rather than raw indicators.

## 2. Input Data & Category-Level Feature Construction
### 2.1 Starting point

Started with the feature-engineered panel with one row per country–year–indicator, containing among others:
- trend_slope – per-indicator long-term trend (from earlier regression step)
- cagr – compound annual growth rate
- rolling_std_5y – 5-year volatility
- momentum – recent 5-year trend vs long-term
- spike, dip – spike/dip flags
- coef_variation – coefficient of variation
- category – thematic group (Health, Education, WBL, etc.)

To ensure clean categories, categories were harmonised and re-assigned:

- All “A woman can…” legal constraints into a new LegalAutonomy category
- The WBL Pension Score into WBL
- Removing stray indicators from the generic “Other” bucket

This ensured every indicator contributed to a meaningful thematic category before aggregation.

### 2.2 Country–Category Trajectory Summaries

To move from indicator-level noise to interpretable patterns, we collapsed indicators into category-level profiles per country.

For each country and each category (Demographics, Economy, Education, Governance, Health, Labor, LegalAutonomy, WBL) we computed:

1. Average trend (direction)
   - cat_<Category>_trend_slope
   - Mean of trend_slope across all indicators in that category

2. Average annual growth
   - cat_<Category>_cagr
   - Captures sustained growth/decline rather than just slope units

3. 5-year volatility
    - cat_<Category>_rolling_std_5y
    - Measures instability of the category over time

4. Momentum (recent vs long-term)
    - cat_<Category>_momentum
    - Positive = recent acceleration, negative = recent slowdown/regression

5. Event sensitivity
    - cat_<Category>_spike – average spike count
    - cat_<Category>_dip – average dip count

6. Relative dispersion
    - cat_<Category>_coef_variation – normalized variability

This helped reduce noise from individual indicators while preserving thematic structure.
It speaks the language of your storyline: “Is governance becoming more volatile? Are legal reforms accelerating? Are demographic indicators stabilizing?”
It keeps the feature space manageable (~60 country-category metrics) and interpretable.

### 2.3 Handling Missing & Extreme Values

Mosly no nulls were found except for a few momentum columns (cat_Governance_momentum, cat_LegalAutonomy_momentum, cat_WBL_momentum) for countries with very short time windows.

Because these were few and not structurally meaningful gaps, we used a simple, transparent imputation. 

This:
- Replaced any infinite values with NaN
- Filled remaining NaN with the feature-wise median across all countries

Rationale:
- Median is robust to outliers and keeps the distribution shape.
- Missingness here comes from lack of history, not meaningful signal, so we do not want to invent structure by using fancy models.
- After this step, .isna().sum() confirmed 0 missing values across all trajectory features.

## 3. Scaling & Trajectory PCA

The trajectory features were standardised as there are multiple scales (e.g., spike counts vs. slope units vs. volatility).
PCA and K-Means are distance-based; without scaling, large-scale features would dominate.
Z-scores make each feature contribute equally.

### 3.2 Trajectory PCA

PCA was visualised via:
- Scree plot (variance explained per component)
- Cumulative variance plot (how much total structure is captured with k PCs)

These plots revealed -> PC1_traj ≈ 21.8%, PC2_traj ≈ 13.2%, PC3_traj ~ 10–11%
Thus by ~6 components, you capture ~70–75% of total variance

Interpretation:

- Trajectory behaviour is multi-dimensional – no single axis explains everything.
- However, PC1 and PC2 already reveal a clear structure, enough to visualise country groupings.


### 3.3 Country Distribution in Trajectory PCA Space

The scatterplot of PC1_traj vs PC2_traj revealed:

1. Gulf states like Qatar, Kuwait, Oman: high PC1_traj (strong, relatively coherent positive trajectories) and moderate to high PC2_traj.
2. United Arab Emirates: far to the right with low PC2_traj – a distinct high-capacity reformer trajectory.
3. Morocco & West Bank and Gaza: far left, low PC1_traj and very negative PC2_traj – weak or regressing trajectories with different profile from other Maghreb and Levant states.
4. Egypt, Iraq, Yemen, Syria, Algeria: cluster towards the centre-left – slower or more fragile improvement with mixed category behaviour.
5. Lebanon: mid-right PC1_traj but higher PC2_traj – improving in some categories while stressed in others.

This plot visually confirmed that countries:

- Do not line up on a single “good vs bad” trajectory axis.
- Instead, they occupy distinct zones shaped by combinations of growth, volatility, and momentum across categories.


## 4. Understanding the Trajectory Clusters

This step was for exploring what makes each cluster distinct using:

- Trajectory PCA scatter with cluster colouring – where clusters sit in the trajectory space
- Category-level heatmaps – average z-scores per cluster for:
- Trend slope (direction)
- 5-year volatility
- CAGR (growth)
- Momentum
- Spike counts
- Dip counts
- Radar charts – cluster “shapes” across categories for slope and volatility

<img width="931" height="702" alt="image" src="https://github.com/user-attachments/assets/a7daab94-844d-4c8d-980a-55df14d335eb" />

### 5.1 Cluster Profiles (High-Level Names)

After exploring all plots, the following behavioural labels were analyzed:

- Cluster 0 – “Steady Moderate Improvers”
- Cluster 1 – “Education-Led Social Improvers”
- Cluster 2 – “Fragility-Driven Decliners”
- Cluster 3 – “High-Volatility Reform Accelerators”
- Cluster 4 – “Governance-Unstable Shock States”
- Cluster 5 – “Stable High-Capacity Reformers”


### 5.2 Trend Slope (Direction) – “Where are categories heading?”

##### Plot: Heatmap and per-cluster radar charts of cat_*_trend_slope (z-scores).

What it answered:

- “On average, are demographics, health, governance, legal and labour indicators improving or deteriorating in each cluster?”

Findings:
1. Cluster 0 – Steady Moderate Improvers
    - Mild positive slopes in Economy, Health, Labor
    - Slightly negative or flat in Demographics, Governance, WBL
→ Countries improve gradually in socio-economic and labour outcomes without dramatic institutional shifts.

2. Cluster 1 – Education-Led Social Improvers
    - Strong positive slope in Education and Demographics
    - Health modestly improving
    - Labor, Governance, LegalAutonomy more muted
→ These countries are pulled forward by schooling and demographic transitions rather than legal or economic reforms.

3. Cluster 2 – Fragility-Driven Decliners
    - Negative or flat slopes in Health and Labor
    - Governance slightly positive (likely weak recovery after collapse)
    - WBL and LegalAutonomy positive but modest
→ This is the “fragile recovery / stop–go progress” cluster: some legal progress amid socio-economic deterioration.

4. Cluster 3 – High-Volatility Reform Accelerators

    - Strong positive slopes in WBL, LegalAutonomy, Labor, Health
    - Economy modestly positive
→ These states are pushing structural reforms in labour and legal rights, and health systems are improving strongly.

5. Cluster 4 – Governance-Unstable Shock States

    - Dominant positive slope in Governance
    - Mixed/flat elsewhere
→ On average, they show recovery or restructuring in governance, but other categories lag or move unevenly.

6. Cluster 5 – Stable High-Capacity Reformers
    - Positive slopes across almost all categories, especially Economy, WBL, LegalAutonomy, Labor, Health
→ These are broad-based improvers with systemic reforms across economic, legal, and social dimensions.



### 5.3 5-Year Volatility – “How bumpy is the ride?”

##### Plot: Heatmaps and radar charts of cat_*_rolling_std_5y.

Question answered:

- “Which clusters change smoothly, and which oscillate across categories?”

Findings:

1. Cluster 0 – low-moderate volatility, a bit higher in Economy → gradual shifts.
   
2. Cluster 1 – moderate volatility, especially in Demographics and Education → fast transitions in schooling and fertility.
   
3. Cluster 2 – high volatility in Health and Demographics → consistent with conflict and crisis episodes.
   
4. Cluster 3 – high volatility in Economy, Education and WBL → reforms and policy shocks affecting institutions.
   
5. Cluster 4 – extremely high volatility in Governance and Labor → frequent swings in governance quality and labour indicators, matching political shocks.
   
6. Cluster 5 – moderate, more balanced volatility, with WBL slightly more volatile due to reform bursts, while other categories stay smooth.

##### Volatility plots connect directly to Storyline B: instability vs resilience.


### 5.4 CAGR – “How fast are categories growing on average?”

##### Plot: Heatmap of cat_*_cagr.

Question answered:

- “Ignoring short-term noise, which clusters actually accumulate long-run gains?”

Key patterns:

1. Cluster 0 – small, positive CAGR in most categories → slow but real improvement.

2. Cluster 1 – positive CAGR in Education, Demographics, Health → sustained progress in human development.

3. Cluster 2 – negative CAGR in Demographics and Economy, positive in Governance and WBL → development reversals with partial institutional catch-up.

4. Cluster 3 – strong positive CAGR in Governance, Labor, WBL despite volatility → reforms are not just noise; they accumulate into real gains.

5. Cluster 4 – mixed: big positive governance CAGR but weaker or negative in social categories → heavy focus on institutions amid social stress.

6. Cluster 5 – positive CAGR across most categories, confirming broad-based high-capacity development paths.

##### This chart tied back to the aim of distinguishing “turning points vs cosmetic reforms” by confirming which volatile clusters (3 & 4) actually convert volatility into long-term gains.


### 5.5 Momentum – “Is progress accelerating or stalling now?”

##### Plot: Heatmap of cat_*_momentum.

Question answered:

- “Are recent years better or worse than the long-run trend for each cluster?”

Highlights:

1. Cluster 0 – slightly negative momentum across categories → progress continues but at a slower pace than earlier years.

2. Cluster 1 – positive momentum in Education and Economy → acceleration in education-led growth.

3. Cluster 2 – negative momentum in Governance and Health → recent stagnation or deterioration after partial recovery.

4. Cluster 3 – positive health momentum but negative in Education and Governance → reforms may have peaked, with some fatigue.

5. Cluster 4 – strongly positive momentum in Demographics & Economy but negative in Labor and Health → economic adjustment with social pain.

6. Cluster 5 – strong positive Health momentum and moderate positive in Labor → recent years particularly good for service delivery and employment outcomes.

##### Momentum is crucial for your “risk of regression” classification later: clusters with negative momentum (2 and parts of 0, 3, 4) are the natural candidates for future risk.

### 5.6 Spikes & Dips – “Event sensitivity and shock exposure”

##### Plot 1: Spike and dip heatmaps
##### Plot 2: Combined bar chart – average spike/dip counts per trajectory cluster & category

Questions answered:

- “Which clusters are shaped by crises (dips) vs. reforms (spikes)?”

- “Where do we see structural instability vs targeted policy bursts?”

Findings:

1. Cluster 0 – Steady Moderate Improvers

    - Few spikes and few dips → systems change gradually, not event-driven.

2. Cluster 1 – Education-Led Social Improvers

    - Elevated spikes in Education and Demographics, small dips → discrete leaps in schooling and social indicators, not crisis-driven.

3. Cluster 2 – Fragility-Driven Decliners

    - High dip counts in Governance, LegalAutonomy, WBL, and elevated negative spikes → instability is downward, reflecting crises and institutional breakdown.

4. Cluster 3 – High-Volatility Reform Accelerators

    - High spike counts in Governance, LegalAutonomy, WBL, Labor with moderate dips → legal and labour reforms come in waves; volatility is mostly upward.

5. Cluster 4 – Governance-Unstable Shock States

    - Very high spikes and dips in Governance, Labor, Health, LegalAutonomy, WBL → genuine shock states with swings between reform and regression.

6. Cluster 5 – Stable High-Capacity Reformers

    - Moderate spikes in WBL and LegalAutonomy, very low dips everywhere → countries implement legal reforms in controlled bursts without crisis-type collapses.

##### These plots are the bridge into the event mapping phase: they show where events are likely to matter most (clusters 2, 3, 4, 5) and in which categories.


## 6. Progress Toward the Main Narrative

Putting everything together, this phase delivers a crucial second pillar of the overall story:

#### Structural clustering (previous phase) -> Answered: “Who looks similar today?”

Identified level-based clusters and structural anomalies (Lebanon, West Bank & Gaza, Yemen).

#### Trajectory clustering (this phase)
Answered: 
- “Who moves similarly over time?”
- “Which countries convert reforms into sustained gains vs those stuck in fragile cycles?”
- “Where do shocks hit hardest: governance, health, legal rights, or demographics?”

Now a clear distinction of country trajectory types grounded in slopes, volatility, spikes, dips, growth, and momentum is found:

- Reform-driven volatility (Clusters 3 & 5)
- Fragility-driven volatility (Cluster 2 & 4)
- Slow, steady improvers (Clusters 0 & 1)


# ♦︎ Phase 9 – Indicator-Level Behavioral Clustering

Location: notebooks/indicator_clustering.ipynb
Inputs: Feature-engineered panel from feature_eng.ipynb (country–year–indicator) reduced to one row per indicator through multi-dimensional summary statistics.

## 1. Purpose & Link to the Main Storyline

While earlier phases focused on countries, this stage asks a different question:

- “How do indicators themselves behave over time?”

That is:
- Which gender, economic, and legal indicators are stable, which are volatile, and which are shock-sensitive?
- Which indicators move due to long-term structural change, and which move due to policy reforms or crises?
- Where do spikes and dips concentrate across thematic dimensions?

This directly supports:

#### Sub-Storyline A – Long-Term Trajectories

(Indicators with sustained slopes, stable CAGR, consistent momentum)

#### Sub-Storyline B – Instability vs Resilience

(Indicators with volatility, spikes, dips, and a high coefficient of variation)

And expands:

#### Main ML Goal 2 – Identifying Behavioural Indicator Types

Indicators are classified into meaningful behavioural groups that contextualise shocks, reforms, and long-term development patterns across MENA.

## 2. Input Data & Indicator-Level Feature Construction

### 2.1 Starting Data

From feature_eng.ipynb, each country–year–indicator row originally included:
- trend_slope
- cagr
- rolling_std_5y
- momentum
- spike, dip
- coef_variation
- regional_deviation, regional_deviation_std
- event_global, event_country
- category
- And all baseline numeric outputs (val_num, yoy_change, etc.)

These were the building blocks for behavior classification.


### 2.2 Indicator-Level Aggregation

Rather than clustering thousands of rows, clustering was performed at the indicator level.

For each indicator (indicator_name), we computed:

1. Volatility Measures
    - avg_volatility = mean(rolling_std_5y)
    - coef_variation = mean(coef_variation)

2. Directional Trends
    - slope_mean = mean(trend_slope)
    - cagr_mean = mean(cagr)
    - momentum_mean = mean(momentum)
(captures whether recent trends accelerate or decelerate)

3. Event-Driven Behavior
    - spike_count = sum(spike)
    - dip_count = sum(dip)
    - global_event_count = number of non-null global events
    - country_event_count = number of non-null country-level events

4. Cross-sectional Stress
    - regional_dev_std = mean(regional_deviation_std)

5. Category Assignment
    - One dominant category was assigned using the mode of the indicator.

This results in 42 indicators × 11 feature dimensions, producing the input matrix for clustering.


### 2.3 Handling Missing & Extreme Values

During extraction, several indicators contained inf values (from division in CAGR or CV) while others had some NaN due to short time spans. 

Median imputation was used as it is robust to outliers and aligns with the non-mechanistic nature of missingness (short histories, definition constraints).
After this, .isna().sum() confirmed no missing values.


## 3. Scaling & PCA for Indicator Behaviour

### 3.1 Standardisation

All numerical features were standardised using z-scores:
- Prevents large-scale features (e.g., slope vs spikes) from dominating distance metrics
- Necessary for both PCA and K-Means
- Ensures each behavioral dimension contributes equally

### 3.2 PCA: Extracting Behavioral Dimensions

PCA extracted underlying behavioral axes:

#### ⭐ PC1 = Structural Trend & Long-Term Direction

Dominated by:
- slope_mean
- cagr_mean
- momentum_mean

##### High PC1 → indicators with strong sustained improvements
##### Low PC1 → declining or stagnant indicators


#### ⭐ PC2 = Volatility & Shock Sensitivity

Dominated by:
- avg_volatility
- spike_count
- dip_count
- coef_variation

##### High PC2 → indicators strongly reacting to crises and policy shocks
##### Low PC2 → very stable indicators


PCA confirmed two major behavioral forces:
1. Structural movement
2. Event-driven instability


## 4. Understanding the Indicator Clusters

Eight distinct behavioral groups emerged.
Cluster names below reflect their behavioral signatures.

### 1. Cluster 0 – “Gradual Structural Demographic Shifts”

Indicators:
    - Life expectancy (M/F)
    - Employment ratios
    - Female household headship
    - Birth rate

Characteristics:
    - Slow-moving
    - Moderate volatility
    - Not driven by events

##### Interpretation:
Long-term demographic shifts that evolve gradually and independently of political shocks.

### 2. Cluster 1 – “Highly Volatile, Reform-Sensitive Indicators”

Indicators:
    - Inflation
    - Seats held by women in parliament
    - Multiple WBL subindices
    - Secondary education GPI
    - Vulnerable employment

Characteristics:
    - Extreme volatility
    - High spike frequency
    - React quickly to reforms and crises

##### Interpretation:
This cluster captures event-reactive indicators that move sharply during Arab Spring, COVID-19, conflicts, economic turmoil, and periods of legal reform.

### 3. Cluster 2 – “Scale Outlier: Population Dynamics”

One indicator: Total population

##### Population eclipses all others in magnitude and behavior → isolated in its own cluster.

### 4. Cluster 3 – “Stable Legal Rights (One-Time Reforms)”

Indicators:
    - Mobility rights
    - Passport rights
    - Workplace restrictions
    - Divorce rights
    - Violence protections

Characteristics:
    - 0 dips, minimal volatility
    - Mostly binary indicators that change once when laws pass

##### Interpretation:
Stable legal rights that exhibit monotonic improvements with no cyclical behavior.


### 5. Cluster 4 – “Declining Health & Fertility Indicators”

Indicators:
    - Adolescent fertility
    - Maternal mortality
    - Crude death rate

Characteristics:
    - Strong negative slopes
    - Development-driven transitions

##### Interpretation:
Structural declines associated with demographic and health system improvements.


### 6. Cluster 5 – “Growing Participation & Economic Capacity Indicators”

Indicators:
    - GDP per capita
    - Women in parliament
    - WBL parenthood & workplace indices

Characteristics:
    - Strong positive trend
    - Moderate volatility
    - Reform + economic cycles

##### Interpretation:
High-growth institutional and economic indicators that combine structural improvement with reform bursts.

### 7. Cluster 6 – “Fully Stabilized Legal Rights”

Indicators:
    - Baseline rights (bank account, contract signing, residency, etc.)
    - Pension rights
    - Vulnerable male employment

Characteristics:
    - Zero slope for many
    - Rights already uniformly granted

##### Interpretation:
Indicators that reached full saturation before 2000; do not change over time.

### 8. Cluster 7 – “Labor Market Stress & Cyclical Unemployment”

Indicators:
    - Female unemployment
    - Male unemployment

Characteristics:
    - Moderate volatility
    - Clear business cycle response
    - Sensitive to conflict & economic recessions

##### Interpretation:
Labor market stress indicators showing cyclical and crisis-driven fluctuations.


## 5. Cluster Summary Table


| **Cluster ID** | **Cluster Name** | **Description** | **Example Indicators** |
|----------------|------------------|------------------|-------------------------|
| **0** | **Gradual Structural Demographic Shifts** | Slow-moving, long-term demographic & labor indicators with moderate volatility and minimal event sensitivity. | Life expectancy (M/F), Birth rate, Employment-to-population ratio |
| **1** | **Highly Volatile, Reform-Sensitive Indicators** | Strongly affected by reforms, crises, and policy cycles; high spikes/dips and large variance. | Inflation, WBL indicators (Mobility, Pay, Marriage, Entrepreneurship), Seats held by women, Education GPI |
| **2** | **Scale Outlier: Population Dynamics** | Unique large-scale behavior; mathematically incomparable to other indicators. | Total population |
| **3** | **Stable Legal Rights (One-Time Reforms)** | Binary legal rights that change only when laws pass; extremely stable over time. | Passport rights, Mobility inside country, Industrial job rights, Divorce rights, Domestic violence legislation |
| **4** | **Declining Health & Fertility Indicators** | Strong negative long-term slopes; structural demographic transitions. | Maternal mortality, Adolescent fertility, Crude death rate |
| **5** | **Growing Participation & Economic Capacity Indicators** | Strong positive trends, moderate volatility; reflect economic cycles + reform progress. | GDP per capita, Women in parliament, WBL Parenthood & Workplace indices |
| **6** | **Fully Stabilized Legal Rights** | Rights already granted across all years; almost zero slope & volatility. | Right to open bank account, Sign contract, Register business, Remarriage rights |
| **7** | **Labor Market Stress & Cyclical Unemployment** | Sensitive to economic conditions, conflict, and recessions; moderate volatility. | Female unemployment, Male unemployment |




## 6. Progress Toward the Main Narrative

This phase delivers the second analytical pillar of your project:
- Country clustering → who moves similarly?
- Indicator clustering → what moves similarly?

Together, they answer:
1. Which indicators react most to crises?
2. Which indicators show steady structural progress?
3. How do reforms propagate across categories?
4. Which indicators drive divergence between MENA countries?

This will directly feed:

✔ Event overlay analysis
✔ Risk-of-regression classification
✔ Tableau storytelling (indicator cluster filters)
✔ Thesis chapters on shock sensitivity and resilience


# ♦︎ Phase 10 – Event Overlay & Shock Sensitivity Analysis

Location: notebooks/Event_Overlay.ipynb
Inputs: Fully cleaned + feature-engineered panel dataset (country–year–indicator) with spikes, dips, volatility metrics, trend metrics, and event flags.

## 1. Purpose & Link to the Main Storyline

This phase introduces the temporal and political dimension of the MENA indicator system:

“How do countries and indicators react when real-world events occur?”
“Which countries are resilient, reactive, or shock-sensitive?”
“Which thematic areas respond to crises, and which respond to reform windows?”

Up to Phase 9, these insights were found:
- Country similarity structures (country clustering)
- Indicator behavior types (indicator clustering)
- Long-term trajectories (trend slopes, CAGR, momentum)
- Volatility structure (rolling std, coefficient of variation)

Phase 10 integrates actual historical events into the analytical narrative, enabling the connection between  quantitative volatility and political timelines, such as:
- Arab Spring (2011–2013)
- 2014 Oil Crisis
- Syrian War escalation
- Yemen Civil War
- Libyan conflict
- COVID-19 (2020–2021)
- National legal reforms (WBL reforms, constitutional changes)

This allows the project to answer core storyline questions:
1. Sub-Storyline C — Crisis & Reform Sensitivity
    - How do governance, health, labor, and legal indicators respond to systemic shocks?
    - Do crises produce dips, or do reforms produce spikes?
    - Are some categories structurally resistant to instability?

2. Sub-Storyline D — Divergence of National Trajectories
    - Why did some countries deteriorate sharply during conflict (Libya, Syria, Yemen) while others barely moved?
    - Are zero-movement countries (Egypt, Yemen in some indicators) experiencing data suppression, missingness, or political reporting constraints?


## 2. Country-Level Event Impact Summaries

For each country, the following was generated:

- Summary table of all indicators
- Annual spike/dip counts
- Event-year overlays
- Category-specific response charts
- Multi-page PDF report (final deliverable)

Each PDF now includes:
- Country name + cluster placement
- Clean tables (auto-fitted, wrapped text)
- Category-by-category plots
- Event-aligned spike/dip timelines

This produces the first fully interpretable narrative deliverable of the project.


## 3. New Analytical Layers Introduced in This Phase

### 3.1 Event Sensitivity Score (ESS)

``` bash
ESS = (Spikes + Dips during event years) / Total indicators
```

This immediately revealed:
- GCC → reform-sensitive but crisis-resistant (spikes > dips)
- Egypt & Yemen → zero ESS for multiple categories
    → indicating non-reporting, missing annual updates, or
political data stagnation, not genuine stability.

### 3.2 Category–Event Alignment Matrix

This directly answers:

##### “Which thematic areas respond to which event types?”

Example structural finding:

| Category         | Arab Spring | Oil Crisis | COVID-19   | Local Political Events |
| ---------------- | ----------- | ---------- | ---------- | ---------------------- |
| Health           | dip-heavy   | flat       | dip-heavy  | variable               |
| Governance       | flat        | flat       | spike-heavy| highly volatile        |
| Labor            | mixed       | flat       | mixed      | dip                    |
| WBL Legal Rights | flat        | minimal    | spike-heavy| spike                  |


### 3.3 Event Typology Differentiation (Crisis vs Reform)

Using spikes/dips and categories:

Crises → dips in Health, Labor, Governance

Reforms → spikes in WBL indicators

Global shocks (COVID-19) create synchronized declines across all countries

Local conflicts produce extreme dips

This brings the conclustion that:

##### Indicators behave differently depending on whether the event is a crisis or a reform.

## 4. Key Findings & Emerging Narrative

This phase reveals fundamental structural differences between countries and categories:

### 4.1 Cross-Country Insight
- Libya, Syria, Yemen → collapse-type responses
- Lebanon → economic crisis volatility
- GCC → reform-driven spikes, minimal dips
- Egypt → unnaturally flat pattern
        → strong indicator of data stagnation
- Morocco, Tunisia, Jordan → moderate sensitivity with selective reform impacts

### 4.2 Category-Level Insight

- WBL → responds only to reforms, not crises
- Health → highest dip concentration
- Labor → chronically unstable
- Governance → mirrors national political trajectories

### 4.3 Event-Type Insight

- Arab Spring → mixed crisis/reform signal
- Oil Shock 2014 → broad economic deterioration
- COVID → universal health collapse
- Local conflicts → persistent, deep structural dips


## 5. Deliverables Produced in This Phase

### 1. Full Country PDF Reports

Each includes:
- Summary table
- Category breakdown
- Spike & dip visualization
- Event annotations

### 2. Global Event Summary Report

Contains:
- Event Sensitivity ranking
- Category–Event Alignment Matrix
- Crisis vs Reform typology table

## 6. How This Phase Advances the Main Thesis

It explains why the data behaves the way it does.

These questions can be now answered:
1. Why some countries diverge
2. Why certain indicators fluctuate
3. Why some clusters exist
4. Why spikes and dips occur

This turns descriptive analytics into causal political-economic storytelling.



