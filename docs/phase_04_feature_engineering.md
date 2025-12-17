# ♦︎ Phase 4 – Feature Engineering & ML Dataset Construction

## Overview

Following the completion of data cleaning and validation, this phase focused on transforming the cleaned long-format dataset into a **machine-learning–ready feature matrix**. The objective was to enrich the data with semantic context, resolve remaining missingness in a statistically defensible way, and engineer temporal, structural, and shock-sensitive features suitable for downstream modeling.

This phase produced **`ml_df`**, a fully standardized dataset with:

* 0 missing values
* Consistent time-series structure
* Context-aware metadata (region, category)
* Rich temporal and volatility features

---

## 1. Dataset Preparation & Semantic Enrichment

To support interpretable and geopolitically grounded modeling, two high-level contextual variables were introduced **before** feature construction.

### 1.1 Region Mapping

Each country was assigned to one of four regional groupings:

* **GCC**
* **Levant**
* **North Africa**
* **Other** (Iran, Yemen, Iraq)

**Rationale:**
Iran, Yemen, and Iraq do not fully align with the political or geographic boundaries of the other subregions and exhibit distinct structural and conflict-related dynamics.

**Why this matters**

* Enables region-level aggregation and comparison
* Supports region-aware imputation strategies
* Improves interpretability of ML outputs
* Allows geo-sensitive modeling of shocks and reforms

---

### 1.2 Indicator Category Mapping

Each indicator was mapped to a broader thematic category (e.g. Education, Health, Labor, Legal Rights, Demographics, Economy).

**Why this matters**

* Groups indicators logically during feature engineering
* Improves model interpretability and narrative clarity
* Enables category-specific expectations (e.g. volatility differs between legal and economic indicators)

---

## 2. Feature Reduction: Removing Structurally Incomplete Indicators

Certain indicators—most notably the **Human Capital Index (HCI)** series—were excluded from the ML dataset due to **structural non-reporting**, not random missingness.

**Why HCI was removed**

* Published only from 2018 onward
* Many conflict-affected countries have zero valid observations
* Long missing blocks prevent meaningful interpolation
* Inclusion would introduce noise rather than signal

➡️ HCI indicators were **removed from `ml_df`** but retained in visualization outputs to illustrate **data coverage gaps**.

This step significantly improved dataset consistency and modeling reliability.

---

## 3. Missing Value Resolution (Final ML Dataset)

After indicator reduction, a small number of missing values remained. These were resolved using a **two-stage, context-aware imputation strategy**.

### 3.1 Stage 1 — Time-Series Interpolation

Indicators were grouped by `indicator_name × country × year` and interpolated based on missingness class:

* **Moderate missingness** → short-gap interpolation
* **Small gaps** → forward/backward fill only

This reduced missing values from **166 → 105**.

---

### 3.2 Stage 2 — Regional Mean Imputation

Remaining missing values belonged to indicators where:

* interpolation was mathematically inappropriate
* missingness followed regional patterns
* regional socio-economic similarity justified group-based imputation

**Imputation logic**

1. Compute region–year mean per indicator
2. Fill missing values using this regional anchor
3. Apply nearest-year fallback only when strictly necessary

This approach preserves geopolitical structure and is statistically safer than global mean imputation.

**Result:**
➡️ All remaining missing values were resolved (**0 nulls total**).

---

## 4. Feature Engineering

With a clean, complete dataset in place, raw indicator values were transformed into **temporal, volatility-based, shock-sensitive, and contextual features** designed to capture development dynamics rather than static levels.

---

### 4.1 Temporal Dynamics

| Feature           | Purpose                | ML Value                         |
| ----------------- | ---------------------- | -------------------------------- |
| **YoY Change**    | Short-term movement    | Early detection of policy shifts |
| **YoY % Change**  | Scale normalization    | Cross-indicator comparability    |
| **Rolling Means** | Trend smoothing        | Improved model stability         |
| **Rolling STD**   | Volatility measurement | Shock sensitivity detection      |
| **CAGR**          | Long-term growth       | Directional classification       |

---

### 4.2 Shock & Event Features

| Feature      | Meaning                   | ML Role                    |
| ------------ | ------------------------- | -------------------------- |
| **Spikes**   | Sudden increases (> 2 SD) | Detects reform bursts      |
| **Dips**     | Sudden drops (< −2 SD)    | Detects crisis years       |
| **Outliers** | Extreme deviations (IQR)  | Noise control & robustness |

These features are central to understanding how gender indicators react to political, economic, and social shocks across the MENA region.

---

### 4.3 Trend & Stability Features

| Feature                      | Description                      | ML Contribution                     |
| ---------------------------- | -------------------------------- | ----------------------------------- |
| **Trend Slope**              | Linear long-term direction       | Improving vs declining trajectories |
| **Coefficient of Variation** | Normalized volatility            | Stability differentiation           |
| **Momentum**                 | Recent acceleration/deceleration | Turning point detection             |

---

### 4.4 Contextual Features

| Feature                | Purpose                     | ML Role                                              |
| ---------------------- | --------------------------- | ---------------------------------------------------- |
| **Regional Deviation** | Distance from regional mean | Controls for shared shocks (Arab Spring, oil crises) |

By embedding region and category context **before** feature creation, all engineered features retain geopolitical and thematic meaning.

---

## 5. Phase Output & Readiness Check

### Summary of Improvements

| Step                         | Purpose                | Impact                    |
| ---------------------------- | ---------------------- | ------------------------- |
| Region & category enrichment | Contextual grounding   | Improved interpretability |
| Indicator reduction          | Noise prevention       | Cleaner ML signal         |
| Time-series interpolation    | Gap repair             | Realistic trends          |
| Regional imputation          | Structural consistency | 0 missing values          |
| Validation                   | ML readiness           | `ml_df` finalized         |

---

## Conclusion

The final feature-engineered dataset captures **long-term trends, short-term volatility, shock sensitivity, and regional context**, making it suitable for:

* Structural and trajectory-based country clustering
* Classification of high-risk country-years
* Anomaly detection for crisis periods
* Dimensionality reduction (PCA / UMAP)
* Explainable AI (SHAP) to interpret drivers of gender inequality

This phase forms the **analytical backbone** of all subsequent modeling and integration stages.


