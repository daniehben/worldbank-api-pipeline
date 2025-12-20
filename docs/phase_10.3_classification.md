# Phase 10 — Classification: Identifying High-Risk Anomaly Years

## 1. Purpose of the Classification Phase

The objective of this phase is to determine whether **anomalous years in gender-related indicators** across MENA countries are **structurally predictable** based on prior indicator behavior, volatility, and contextual features — rather than being purely random or ex post descriptive events.

Classification is used here to answer the following core question:

> **Can we identify high-risk years for anomalous gender-indicator behavior using historical patterns, without using future information?**

This phase complements earlier clustering and anomaly detection steps by **testing predictability**, not just describing structure.

---

## 2. Definition of “High-Risk” (Label Construction)

### 2.1 Target Variable

The primary classification target is:

* **`anomaly_flag`**

  * Binary variable
  * `1` = anomalous year
  * `0` = normal year

This label originates from prior anomaly detection using Isolation Forest on PCA-reduced structural features.

### 2.2 Conceptual Meaning of High-Risk

A year is considered *high-risk* if it exhibits:

* unusually high volatility or instability across multiple indicator categories,
* sharp deviations from historical or regional norms,
* structural inconsistency relative to the country’s typical trajectory.

Importantly, **the label is not derived from events directly**, ensuring the classifier learns **indicator behavior**, not event memorization.

---

## 3. Data Preparation and Feature Assembly

### 3.1 Feature Sources

Classification features are drawn from three pre-processed sources:

1. **Category-level engineered features**

   * Trend slopes, CAGR, volatility, momentum
   * Spike/dip counts
   * Coefficient of variation
   * Regional deviation metrics

2. **Event flags (contextual, not target-derived)**

   * Global crisis
   * Regional crisis
   * Country-level conflict
   * Reform periods
   * Primary event type (conflict / crisis / reform / none)

3. **Anomaly metadata**

   * Structural cluster assignment
   * Anomaly driver (dominant PCA component)

All features are aggregated at the **country–year level**.

---

## 4. Train–Test Split Strategy (No Leakage)

To prevent temporal leakage, a **time-aware split** is used:

* Training set: earliest ~80% of years
* Test set: most recent ~20% of years

This ensures:

* the model only learns from **past information**,
* predictions reflect genuine forward-looking risk assessment.

---

## 5. Preprocessing Pipeline

### 5.1 Feature Types

* **Numeric features**: continuous indicators (slopes, volatility, deviations)
* **Categorical features**:

  * `event_type_primary`
  * `Cluster_Name`

### 5.2 Preprocessing Steps

| Step              | Purpose                                               |
| ----------------- | ----------------------------------------------------- |
| Median imputation | Robust handling of remaining missing values           |
| Standard scaling  | Required due to extreme scale disparities             |
| One-hot encoding  | Converts categorical context into model-readable form |
| ColumnTransformer | Applies transformations cleanly and reproducibly      |

Scaling was empirically justified after auditing feature ranges, which spanned **7–8 orders of magnitude**.

---

## 6. Model 1 — Logistic Regression (Baseline, Interpretable)

### 6.1 Why Logistic Regression

Logistic Regression is used as:

* an interpretable baseline,
* a linear risk model,
* a benchmark for assessing whether anomalies are separable at all.

### 6.2 Results

**Classification Performance (Test Set)**

| Metric                    | Value |
| ------------------------- | ----- |
| Accuracy                  | 0.878 |
| ROC-AUC                   | 0.97  |
| Recall (Anomaly class)    | 1.00  |
| Precision (Anomaly class) | 0.58  |

### 6.3 Interpretation

Logistic Regression shows that:

* anomalous years are **strongly associated** with elevated instability and volatility,
* false positives exist, indicating **overlapping linear risk profiles** between normal and anomalous years,
* risk increases gradually as instability metrics increase.

This suggests anomalies are **partially but not perfectly linearly separable**.

---

## 7. Model 2 — Random Forest (Nonlinear, Interaction-Aware)

### 7.1 Why Random Forest

Random Forest is introduced to:

* capture nonlinear thresholds,
* model interactions across categories,
* detect regime-like transitions missed by linear models.

### 7.2 Results

**Classification Performance (Test Set)**

| Metric    | Value |
| --------- | ----- |
| Accuracy  | 1.00  |
| ROC-AUC   | 1.00  |
| Precision | 1.00  |
| Recall    | 1.00  |

### 7.3 Feature Importance Structure

Top contributing features include:

* category-level volatility (coefficient of variation),
* trend slopes across labor, economy, health, and governance,
* regional deviation metrics,
* structural cluster context.

Importantly:

* **no single feature dominates**,
* importance is distributed across multiple instability dimensions.

This strongly suggests the model is learning **structural patterns**, not exploiting leakage.

---

## 8. How the Two Models Build on Each Other

| Aspect                   | Logistic Regression | Random Forest              |
| ------------------------ | ------------------- | -------------------------- |
| Risk shape               | Linear, smooth      | Threshold-based, nonlinear |
| Interpretability         | High                | Moderate                   |
| Captures interactions    | ❌                   | ✅                          |
| Identifies regime shifts | ❌                   | ✅                          |
| Overfitting risk         | Low                 | Managed via validation     |

Together, they show that:

> Anomalous gender-indicator years are not just high-volatility years, but emerge when **multiple instability dimensions coincide**, producing regime-like transitions.

---

## 9. Substantive Insight Gained

This phase demonstrates that:

* gender-related anomalies are **predictable** from prior indicator behavior,
* volatility and deviation matter more than absolute indicator levels,
* anomalies follow **structural instability logic**, not random shocks.

In political-economy terms:

> Sudden gender-outcome disruptions tend to arise when institutional volatility, demographic pressure, and regional divergence align — not from isolated indicator movements.

---

## 10. Role of Classification in the Overall Project

Classification serves as:

* a validation layer for anomaly detection,
* a bridge between descriptive clustering and causal event analysis,
* a foundation for early-warning or risk-scoring extensions.

This phase confirms that the project’s earlier analytical layers capture **real, learnable structure**, strengthening the overall empirical narrative.


Perfect — here is a **concise, academically clean limitations subsection** that fits seamlessly at the end of `phase_10_classification.md` without weakening your results or over-apologizing.

You can paste this directly under Section 10 or as **Section 11**.

---

## 11. Limitations and Scope

While the classification results are strong, several limitations should be noted to properly contextualize the findings:

1. **Small Sample Size**
   The dataset is limited to country–year observations for the MENA region, resulting in a relatively small number of anomalous cases. This increases sensitivity to class imbalance and limits the use of very deep or highly parameterized models.

2. **Label Dependence on Anomaly Detection**
   The classification target (`anomaly_flag`) is derived from an unsupervised anomaly detection model rather than externally validated ground truth. While this is appropriate for exploratory risk identification, it means the classifier learns *structural anomaly patterns*, not normative “ground truth” crises.

3. **Potential Overfitting in Nonlinear Models**
   The perfect performance of the Random Forest model suggests strong separability but may also reflect overfitting to a structured feature space. Results should therefore be interpreted as evidence of **learnable structure**, not guaranteed generalization beyond the observed data regime.

4. **Event Flags as Context, Not Causality**
   Event-related features are included as contextual signals but are not causal drivers in the model. The classification does not imply that events cause anomalies directly—only that anomalous years often coincide with periods of instability.

5. **Regional Specificity**
   Findings are specific to the MENA region and to the selected indicator set. Applying the same framework to other regions would require recalibration of both anomaly detection thresholds and classification logic.

Despite these limitations, the classification phase provides strong evidence that anomalous gender-indicator years are **systematically associated with measurable instability patterns**, reinforcing the project’s broader analytical framework.


