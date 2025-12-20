# Phase 10.7 — SHAP Capacity Robustness Analysis

## Objective

The objective of this phase is to evaluate whether the model’s SHAP explanations remain stable **under varying model capacity constraints**, or whether they are artifacts of Random Forest flexibility.

Specifically, this phase asks:

> **Are the SHAP explanations stable because the underlying signal is real, or because the model is sufficiently flexible to fit noise?**

This complements earlier robustness checks (temporal splits and random seeds) by testing **structural robustness** — i.e., robustness to changes in model expressiveness.

---

## Motivation

Tree-based models such as Random Forests can produce convincing explanations even when signals are weak, especially if the model has high capacity (deep trees, many estimators).

If SHAP explanations are:

* **Unstable under capacity reduction**, this suggests reliance on spurious patterns
* **Stable under capacity reduction**, this supports the existence of a genuine underlying signal

Therefore, we test SHAP behavior across **explicitly constrained capacity tiers**, holding data, features, and labels constant.

---

## Experimental Design

### Capacity Tiers

Four Random Forest capacity tiers were defined, all sharing the same base configuration:

```python
n_estimators = 300
class_weight = "balanced"
random_state = 42
```

The tiers differ only in structural constraints:

| Tier Name    | max_depth | min_samples_leaf | Interpretation           |
| ------------ | --------- | ---------------- | ------------------------ |
| T0_reference | None      | 5                | Full-capacity reference  |
| T1_mild      | Moderate  | Increased        | Mild constraint          |
| T2_moderate  | Lower     | Higher           | Stronger regularization  |
| T3_strong    | Very low  | Highest          | Aggressively constrained |

This design isolates **capacity effects** without introducing confounding randomness.

---

## SHAP Extraction Procedure

For each capacity tier:

1. The Random Forest model was trained on the same training split
2. Predictions were generated on the same test set
3. SHAP values were computed using `shap.TreeExplainer`
4. SHAP values were extracted for the positive class
5. Feature-level summaries were computed:

   * Mean absolute SHAP value
   * Rank ordering of features
6. Results were stored in long-format tables for comparison

All tiers produced valid SHAP outputs with identical feature dimensionality.

---

## Diagnostic Analyses

Four complementary diagnostics were performed.

---

### 1. Feature Rank Stability Across Capacity Tiers

**Question:**
Do the most important features change when model capacity is constrained?

**Method:**

* Pivot table of `feature × tier → rank`
* Rank volatility computed as standard deviation across tiers

**Findings:**

* The top explanatory features remain **rank-invariant** across all tiers
* Core drivers (e.g., legal autonomy variation, health volatility, labor trends) retain their relative importance
* Only minor rank movement (±1) occurs in mid-ranked features

**Interpretation:**

> Model capacity does not alter the explanatory hierarchy, indicating structural robustness.

---

### 2. Rank Volatility Quantification

**Question:**
Which features are most sensitive to capacity constraints?

**Method:**

* Rank standard deviation computed across tiers

**Findings:**

* Most features exhibit zero or near-zero rank volatility
* No feature displays unstable or chaotic ranking behavior

**Interpretation:**

> Explanations are not reorganized when model expressiveness is reduced, ruling out overfitting-driven ranking artifacts.

---

### 3. SHAP Magnitude Drift Across Capacity Tiers

**Question:**
Does explanatory strength collapse or behave erratically under constraint?

**Method:**

* Mean absolute SHAP values plotted across tiers
* Feature-wise trend inspection

**Findings:**

* SHAP magnitudes change **smoothly**, not abruptly
* No feature disappears or dominates suddenly
* Minor attenuation under stronger constraints is observed, as expected

**Interpretation:**

> The explanatory signal degrades gradually with capacity reduction, consistent with genuine signal rather than noise fitting.

---

### 4. SHAP Concentration Analysis

**Question:**
Does the model rely on fewer features when constrained?

**Method:**

* Sum of mean absolute SHAP values for top-5 features computed per tier

**Findings:**

* SHAP concentration remains stable or slightly decreases
* No collapse onto a small subset of features is observed

**Interpretation:**

> Capacity constraints do not force the model into brittle, single-feature explanations.

---

## Summary of Findings

Across all diagnostics:

* Feature rankings are stable
* SHAP magnitudes drift smoothly
* Explanatory power does not collapse
* No concentration spike occurs under constraint

Together, these results indicate:

> **SHAP explanations are stable because the signal is real, not because the model is overly flexible.**

---

## Methodological Implications

This phase provides **structural robustness validation**, complementing:

* Temporal robustness (cutoff years)
* Random seed robustness
* Event-based explanation analysis

Together, these establish that:

* The classifier learns meaningful relationships
* SHAP explanations reflect consistent structural patterns
* Interpretability results are not artifacts of model tuning


