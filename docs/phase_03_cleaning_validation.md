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

```shell
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
```

✔ Confirmed expected columns:

```shell
country_name | year | indicator_id | indicator_name | value | unit_type | source | source_reliable
```

##### 2.Identify Completely Empty Indicators (Structural Missingness)

##### 3. Deep-dive into Education (UNESCO + WHO) Data to Assess Coverage.

Since education & literacy were known to be sparse: a) Created a source flag (WHO vs UNESCO)

Checked which indicators came from which source:

```shell
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

```shell
who_coverage = who_only["value"].isna().mean()
unesco_coverage = unesco_only["value"].isna().mean()
```

Result:

- WHO avg missingness ≈ 17%
    
- UNESCO avg missingness ≈ 14% (but extremely patchy by country)
    

c) Country-level coverage patterns (major insight)

|Country|% Missing|
|---|---|
|Libya|**100%**|
|Yemen|92%|
|Saudi Arabia|100%|
|UAE|92%|
|Bahrain|81%|
|Egypt|68%|
|Morocco|88%|
|Tunisia|50%|
|Iran|36%|
|West Bank & Gaza|18%|

➡ Conclusion: Education data is severely limited, especially for GCC + conflict-affected countries. ➡ Therefore: UNESCO / WHO education indicators cannot be used for ML but are extremely valuable for the dashboard narrative (data deserts).

##### 4. Classify Missingness Into 4 Categories:

```shell
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

|Indicator|Missing %|Category|
|---|---|---|
|Contraceptive prevalence (modern)|0.87|**Severely incomplete**|
|Gini Index|0.86|**Severely incomplete**|
|School enrollment GPI|0.45–0.60|High/moderate|
|HCI (all versions)|0.38–0.70|Moderate/High|
|Fertility, Life expectancy, Employment, GDP|<5%|Low|

##### 5. Rule-Based Missing Value Treatment (Your Final Framework)

|**Missingness Class**|**% Null**|**Rule Applied**|**Why**|
|---|---|---|---|
|**Low**|<20%|`interpolate_full`|Smooth, stable indicators → safe to interpolate fully|
|**Moderate**|20–45%|`interpolate_safe` (limit=3)|Fill short gaps only|
|**High**|45–75%|`interpolate_short` (limit=1)|Avoid overfitting trends|
|**Severely Incomplete**|>75%|**keep_raw**|Too sparse → used only to visualize data gaps|

##### 6. Apply Cleaning Rules Per Indicator × Country

```shell
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

✔ Missing values reduced to 282 ✔ Fully cleaned + interpolated ✔ Ready for feature engineering + ML modeling (clustering, classification, PCA, SHAP)

B. Tableau Dataset (final_df_for_tableau)

Merged cleaned WB + cleaned UNESCO + raw UNESCO (for missingness transparency)

This dataset:

- Keeps all indicators, even those with 100% missing
    
- Includes a source and source_reliable tag
    
- Preserves missingness for your data desert story
    
- Perfect for Tableau visuals