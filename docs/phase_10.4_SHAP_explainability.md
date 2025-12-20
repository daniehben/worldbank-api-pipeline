# SHAP Explainability Phase

## Interpreting High-Risk Classification Decisions

### Purpose of the SHAP Phase

Following the implementation and evaluation of Logistic Regression and Random Forest classifiers, a SHAP (SHapley Additive exPlanations) analysis was conducted to interpret *why* specific country–year observations were classified as high risk. While model performance metrics confirmed strong predictive accuracy—particularly for the Random Forest model—these metrics alone do not provide insight into the underlying drivers of risk classification.

The SHAP phase was therefore designed to answer a distinct and policy-relevant question:

> **Which structural and temporal indicators drive the model’s high-risk classifications, and how do these drivers vary across global events?**

This phase shifts the focus from *prediction* to *interpretation*.

---

## Step A — Local Explanations: Why Was a Country Flagged in a Specific Year?

The first step focused on **local SHAP explanations**, examining individual predictions to understand the model’s reasoning at the country–year level.

Three representative cases were analyzed:

* A **true positive** (correctly classified high-risk year)
* A **borderline high-risk case**
* A **true negative** (correctly classified low-risk year)

SHAP waterfall plots revealed that high-risk classifications were not driven by a single indicator, but rather by the **combined effect of volatility and trend instability across multiple domains**.

Across true positive cases, the most influential contributors consistently included:

* Legal autonomy volatility (`cat_LegalAutonomy_coef_variation`)
* Cross-indicator instability (`coef_variation_mean`)
* Regional divergence (`regional_deviation_std_mean`)
* Deteriorating or unstable trends in health, education, economy, labor, and demographics

In contrast, true negative cases were characterized by **stabilizing demographic growth and reduced volatility**, which exerted downward pressure on predicted risk.

This confirmed that the model does not flag risk due to absolute indicator levels, but rather due to **structural instability and trend disruption**.

---

## Step B — SHAP Drivers in Relation to Events

To contextualize model explanations within real-world dynamics, SHAP values were aggregated and compared across event flags, including:

* Global crises (e.g., COVID-19)
* Reform periods
* Country-level conflict years

### Event Coverage in the Test Set

Event distribution within the test set was uneven:

* **Global crises:** 36 observations
* **Reform periods:** 3 observations
* **Country-level conflict:** 0 observations

As a result, quantitative SHAP–event comparisons were feasible **only for global crises**, while reform and conflict periods were interpreted qualitatively.

---

### SHAP Patterns During Global Crises

During global crisis years, the dominant SHAP drivers were remarkably consistent:

* Legal autonomy volatility
* Regional deviation from peer countries
* Overall coefficient variation (system-wide instability)
* Negative or unstable trends in economy, education, labor, and health

This pattern indicates that during global shocks, countries are flagged as high risk not because of isolated sectoral declines, but due to **simultaneous stress across legal, economic, and social systems**.

> **Interpretation:**
> Global crises amplify structural fragility. The model responds by weighting volatility and cross-domain instability more heavily than any single indicator.

This directly aligns the machine learning output with the project’s political–economic narrative of systemic vulnerability.

---

### Reform Periods and Conflict Years

Reform periods were sparsely represented in the test set and rarely predicted as high risk. This suggests that reforms alone do not trigger high-risk classification unless accompanied by broader instability across indicators.

Country-level conflict years were not present in the test window and therefore could not be evaluated within the SHAP–event framework.

These limitations reflect data availability rather than model weakness and are documented transparently.

---

## Step C — What the SHAP Phase Has Already Answered

At this stage, the SHAP analysis has conclusively answered several core research questions:

1. **What does “high risk” mean in the model’s logic?**
   → High risk reflects *multi-dimensional instability*, not low performance in a single sector.

2. **Are predictions driven by interpretable, policy-relevant indicators?**
   → Yes. Legal volatility, demographic instability, regional divergence, and sectoral trend disruption dominate explanations.

3. **Do model explanations align with known global shocks?**
   → Yes. Global crises show a distinct SHAP signature centered on systemic instability.

4. **Is the model overfitting to noise or memorizing labels?**
   → No. SHAP explanations are consistent across cases and coherent across domains.

---

## Why the Random Forest Will Be Re-Run

Despite achieving near-perfect classification performance, the Random Forest model will be **re-run intentionally** as part of a robustness and sanity-check phase.

This is not to improve accuracy, but to verify **explanation stability**.

Specifically, the model will be re-estimated with:

* Different random seeds
* Slightly adjusted temporal cutoffs

The objective is to confirm that:

* Dominant SHAP drivers remain stable
* The explanatory narrative does not change materially
* The model’s policy interpretation is robust to minor perturbations

This step is essential because a perfectly scoring model without stable explanations would undermine interpretability claims.

---

## Positioning of SHAP in the Overall Project

The SHAP phase serves as the **bridge between machine learning and political analysis**. It translates statistical classification into interpretable narratives about instability, reform, and crisis vulnerability across MENA countries.



