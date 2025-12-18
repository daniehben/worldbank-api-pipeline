# Pre-Classification Preparation

This stage converts two qualitative signals — **event context** and **country-level anomaly behavior** — into clean, model-ready features for the classification phase. The goal is to ensure the classifier receives **structured, interpretable inputs** rather than raw narrative text.

## 1) Event Flags Construction (Event Overlay)

### 1.1 Available event sources in the dataset

The dataset contains multiple event-text columns, each representing a different scope:

* **short_event**: a small set of standardized global labels (e.g., *Arab Spring, Oil Crisis, SDG, COVID-19, Ukraine War*).
* **country_event_name**: country-specific narratives (fine-grained; may include reforms, protests, attacks, etc.).
* **global_event_name**: region/global summaries (broader, multi-country events grouped into one string).
* **major_event_name**: higher-level global/major events (war, crises, revolutions, etc.).

### 1.2 Flag logic design (reform vs crisis vs conflict)

Rather than manually labeling each event row, the system classifies event text using keyword/regex patterns. Flags are created as binary indicators per country-year:

* **Conflict**: war/civil war/attacks/ISIS/occupation/escalation terms
* **Crisis**: economic/financial crisis, pandemic, inflation surge, recession, collapse terms
* **Reform**: reform, liberalization, policy shift, SDG rollout, legal code changes terms

Because a single year can include multiple signals, we also define a **priority rule** for a single “dominant” label.

### 1.3 Flag outputs

The event overlay produces the following classification-ready country-year features:

* `flag_country_conflict`
* `flag_country_crisis`
* `flag_global_crisis`
* `flag_regional_crisis`
* `flag_reform_period`

### 1.4 Primary event type (single label per row)

To make the signals easier for classifiers (and for later interpretation), we derive:

* **event_type_primary** ∈ {`conflict`, `crisis`, `reform`, `none`}
  using the priority rule:

**conflict > crisis > reform > none**

This upgrades the feature space by:

* reducing ambiguity (one dominant category per year),
* improving interpretability in model results,
* giving a single categorical signal that can be one-hot encoded.

### 1.5 Export artifact from event overlay

Since the full dataframe includes narratives and indicator values, we export a **lean table** that contains only merge keys + event flags:

**event_flags_table**

* keys: `country_name`, `year`
* event features: the five flag columns (+ optionally `event_type_primary`)

This file becomes a direct input to the classification notebook.

---

## 2) Event Flag Validation & Audit Checks (before classification)

Before using event flags as predictors, we run sanity checks to ensure the crisis/conflict signals aren’t over-triggering:

### 2.1 Coverage / distribution audit

* Count how many country-years are flagged for each category
* Check overlap between conflict and crisis using cross-tabulation
  (to understand how often both fire together)

### 2.2 “Too broad” audit using indicator spot-checks

We test whether flagged years correspond to expected indicator shifts by comparing summary statistics for selected indicators under flagged vs non-flagged rows.

Example indicators used:

* Maternal Mortality
* GDP per capita
* Female employment (or labor participation proxy)

Interpretation goal:

* If flags are meaningful, we often expect **directional differences** or **variance shifts** (not necessarily huge, but consistent).

Note: these audits validate **plausibility**, not causality — they are a guardrail against flags being “always-on.”

---

## 3) Anomaly Output Cleaning for Classification

### 3.1 What the anomaly dataframe contains

The anomaly output is country-level (one row per country) and includes:

* `Cluster`, `Cluster_Name`: structural grouping output used for interpretation/context
* `PC1`, `PC2`, `PC3`: reduced-dimension representation of country indicator behavior
* `iso_raw_score`, `iso_anomaly_score`: Isolation Forest anomaly scoring outputs
* `iso_label`: model anomaly label (0 = normal, 1 = anomaly)
* `Anomaly_Driver`: which principal component most contributed (for interpretability)

### 3.2 Derived classification features

Classification needs two simple anomaly features:

1. **anomaly_flag**
   Binary: `1` if anomaly detected, else `0`
   Derived directly from `iso_label`.

2. **anomaly_intensity**
   Continuous “strength” of anomaly signal.
   Logic:

* If `anomaly_flag == 1`: keep the (positive) anomaly magnitude
* If `anomaly_flag == 0`: set to `0` (to avoid injecting noise from non-anomalies)

### 3.3 Sanity checks

* Confirm only flagged countries have non-zero intensity
* Confirm the count of anomalies is plausible (not 0, not most countries)

### 3.4 Export artifact from anomaly notebook

Export a **clean anomaly feature table** for classification:

**anomaly_features.csv**

* keys: `country_name`
* features:

  * `anomaly_flag`
  * `anomaly_intensity`
  * (optional context columns for interpretation: `Cluster_Name`, `Anomaly_Driver`)

---

## 4) Final Inputs for the Classification Notebook

At the start of classification, the notebook should load:

1. **ML dataset / engineered indicators** (your main modeling base)
2. **event_flags_table** (country-year event context features)
3. **anomaly_features.csv** (country-level anomaly predictors)

Merge logic:

* merge event flags on: `country_name`, `year`
* merge anomaly features on: `country_name`

This ensures the classifier sees:

* **what changed in the data** (engineered indicators),
* **what was happening historically** (event flags),
* **which countries behave unusually overall** (anomaly features).

-