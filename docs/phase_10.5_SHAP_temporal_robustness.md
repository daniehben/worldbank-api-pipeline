## Temporal Robustness of SHAP Explanations

### Objective

This section evaluates whether the **explanatory logic of the Random Forest classifier** is stable across time. Rather than assessing predictive performance alone, the goal is to determine whether the **features driving high-risk classifications remain consistent** when the model is trained on different historical windows.

Specifically, we test whether SHAP-based explanations:

* Depend disproportionately on very recent years, or
* Reflect stable, structural drivers that persist across time.

To do this, the model is retrained multiple times using different **training cutoff years**, while keeping:

* The same feature set
* The same target (`anomaly_flag`)
* The same preprocessing and model configuration

Only the **temporal scope of the training data** is altered.

---

### Experimental Design

For each cutoff year (2009, 2014, 2016, 2017, 2018, 2019, 2020):

* The model is trained on data up to and including the cutoff year.
* Predictions and SHAP explanations are computed on post-cutoff observations.
* Mean absolute SHAP values are aggregated per feature.
* Feature importance ranks are computed within each cutoff year.

This produces a comparable set of SHAP explanations across multiple temporal training regimes.

---

### Results and Interpretation

#### 1. SHAP Importance Drift Over Time

The SHAP drift line plot shows how the **global importance of key features** evolves as the training cutoff year advances.

**Finding:**
Core drivers—such as legal autonomy volatility, health-related variability, and labor trend dynamics—exhibit **remarkably stable SHAP magnitudes** across cutoff years, with only minor fluctuations.

**Interpretation:**
This indicates that the model’s explanatory logic does not rely on short-term temporal artifacts. Instead, it consistently identifies the same structural features as influential, regardless of how recent the training window is.

---

#### 2. Feature Rank Stability Across Cutoffs

The feature-rank heatmap compares how features are ranked by importance under different temporal training regimes.

**Finding:**
High-impact features maintain consistently strong ranks across cutoff years. Lower-ranked features show greater variability, which is expected given their marginal contribution.

**Interpretation:**
The stability of top-ranked features suggests that the model does not “change its mind” about what matters most as newer data is added. This provides strong evidence against temporal overfitting in the explanatory layer.

---

#### 3. Concentration of Model Explanations

The explanation concentration plot tracks the **sum of mean absolute SHAP values for the top features** across cutoff years.

**Finding:**
There is no monotonic increase in concentration over time. While slight peaks occur in specific years, the overall pattern remains stable.

**Interpretation:**
The model does not progressively rely on a narrower set of features as training becomes more recent. This suggests that explanatory diversity is preserved and that no single feature (or small subset) dominates explanations due to temporal bias.

---

### Summary of Temporal Robustness Findings

Taken together, these results show that:

* The **same core features** drive model decisions across time.
* SHAP explanations are **not sensitive to the choice of training cutoff year**.
* The model’s explanatory structure reflects **persistent, structural patterns** rather than transient historical effects.

This supports the validity of SHAP-based interpretations in this setting and justifies their use for substantive analysis.

---

### Motivation for Further Robustness Testing

While temporal robustness addresses sensitivity to historical scope, Random Forest models also involve **stochastic elements** (e.g., bootstrapping and feature subsampling). Therefore, an additional robustness check is required to ensure that explanations are not dependent on a particular random initialization.

The next step evaluates **seed robustness**, asking whether SHAP explanations remain stable across different random seeds while holding the data and cutoff year constant.

