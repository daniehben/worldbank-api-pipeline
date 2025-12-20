# Classification Problem Framing (High-Risk Prediction)

## Purpose in the Project

Up to this point, the pipeline identifies what happened (spikes/dips, volatility, anomalies) and when it happened (country-year patterns with event context). The classification step adds the missing layer: **whether these abnormal country-years are systematically predictable from measurable signals**, and **which feature families are most associated with elevated risk**.

This directly supports the project’s main question by moving from descriptive findings (“these country-years are unstable”) to an explainable, testable mechanism (“these feature patterns + event contexts are associated with instability, and the relationship generalizes beyond the training years”).

---

## What “High-Risk” Means (Label Definition)

For this phase, **high-risk is defined as an anomaly-flagged country-year**.

* A country-year is labeled **high-risk = 1** if it is flagged by the frozen anomaly detection model output (`iso_label == 1`).
* Otherwise, it is labeled **high-risk = 0**.

This definition is:

* **Binary and reproducible** (no manual labeling).
* **Consistent with the anomaly pipeline** (classification predicts the anomaly condition rather than redefining risk).
* **Audit-friendly** (the label can be regenerated at any time from the same anomaly model outputs and dataset version).

**Label creation is frozen** for the remainder of the modeling phase to avoid target drift.

---

## What Questions Classification Answers

### Q1 — Predictability

**Can high-risk (anomaly) country-years be predicted from engineered signals and event context, or are they effectively random?**
Answered by: out-of-sample classification performance (e.g., PR-AUC, Recall, F1) under a time-aware split.

### Q2 — Drivers of Risk (Feature Families)

**Which feature families are most associated with high-risk years?**
Feature families:

* Category-wide trajectory signals (trend, CAGR, momentum, volatility)
* Shock signals (spike/dip counts and rates)
* Global summary signals (overall volatility, YoY movement)
* Event context signals (crisis/conflict/reform flags)
* Structural positioning (cluster membership)

Answered by:

* Logistic Regression (directional and stable associations)
* Random Forest (nonlinear effects and interaction patterns)

### Q3 — Event Context vs “Internal” Instability

**Do external events (crises/conflicts/reforms) add predictive power beyond the internal volatility signals?**
Answered by: comparing model performance and feature contributions with and without event flags.

### Q4 — Cluster-Specific Vulnerability

**Do different structural clusters show different risk patterns or sensitivities to shocks?**
Answered by: cluster features in X and how they contribute to predictions (especially via nonlinear models).

---

## Features Used (X) vs Target (y)

### Target (y)

* `anomaly_flag` (binary), derived from anomaly model output (`iso_label`)

### Predictors (X)

Predictors are constructed at the **country-year level** to match the label and the event structure. They include:

* Category-wide engineered features (`cat_*` trend_slope, cagr, momentum, rolling_std_5y, spike, dip, coef_variation)
* Overall country-year summary features (abs YoY change stats, spike/dip/outlier rates, mean volatility and deviations)
* Event flags (`flag_global_crisis`, `flag_regional_crisis`, `flag_country_conflict`, `flag_country_crisis`, `flag_reform_period`)
* Structural cluster membership (`Cluster_Name`)

---

## Leakage Controls (Non-Negotiable)

To preserve validity, the model **must not receive information derived from the anomaly label itself**.

Therefore, the following are treated as **label-derived** and are excluded from X:

* `anomaly_intensity` (derived from anomaly scoring)
* `Anomaly_Driver` (derived from anomaly analysis space)
* Any isolation forest score columns (if present), including raw/anomaly scores

Additionally, evaluation uses a **time-aware split** (training on earlier years and testing on later years) to avoid leaking future patterns into the past.

---

## Modeling Strategy (Why Two Models)

Two model families are used to capture complementary insight:

* **Logistic Regression**: provides interpretable, direction-based relationships (what increases vs decreases risk under linear assumptions).
* **Random Forest**: captures nonlinear behavior and interactions (how combinations like “governance instability + crisis flags” increase risk).



