# ♦︎ Phase 8 – Indicator-Level Behavioral Clustering


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
    - momentum_mean = mean(momentum) (captures whether recent trends accelerate or decelerate)
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

Median imputation was used as it is robust to outliers and aligns with the non-mechanistic nature of missingness (short histories, definition constraints). After this, .isna().sum() confirmed no missing values.

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

Eight distinct behavioral groups emerged. Cluster names below reflect their behavioral signatures.

### 1. Cluster 0 – “Gradual Structural Demographic Shifts”

Indicators: - Life expectancy (M/F) - Employment ratios - Female household headship - Birth rate

Characteristics: - Slow-moving - Moderate volatility - Not driven by events

##### Interpretation:

Long-term demographic shifts that evolve gradually and independently of political shocks.

### 2. Cluster 1 – “Highly Volatile, Reform-Sensitive Indicators”

Indicators: - Inflation - Seats held by women in parliament - Multiple WBL subindices - Secondary education GPI - Vulnerable employment

Characteristics: - Extreme volatility - High spike frequency - React quickly to reforms and crises

##### Interpretation:

This cluster captures event-reactive indicators that move sharply during Arab Spring, COVID-19, conflicts, economic turmoil, and periods of legal reform.

### 3. Cluster 2 – “Scale Outlier: Population Dynamics”

One indicator: Total population

##### Population eclipses all others in magnitude and behavior → isolated in its own cluster.

### 4. Cluster 3 – “Stable Legal Rights (One-Time Reforms)”

Indicators: - Mobility rights - Passport rights - Workplace restrictions - Divorce rights - Violence protections

Characteristics: - 0 dips, minimal volatility - Mostly binary indicators that change once when laws pass

##### Interpretation:

Stable legal rights that exhibit monotonic improvements with no cyclical behavior.

### 5. Cluster 4 – “Declining Health & Fertility Indicators”

Indicators: - Adolescent fertility - Maternal mortality - Crude death rate

Characteristics: - Strong negative slopes - Development-driven transitions

##### Interpretation:

Structural declines associated with demographic and health system improvements.

### 6. Cluster 5 – “Growing Participation & Economic Capacity Indicators”

Indicators: - GDP per capita - Women in parliament - WBL parenthood & workplace indices

Characteristics: - Strong positive trend - Moderate volatility - Reform + economic cycles

##### Interpretation:

High-growth institutional and economic indicators that combine structural improvement with reform bursts.

### 7. Cluster 6 – “Fully Stabilized Legal Rights”

Indicators: - Baseline rights (bank account, contract signing, residency, etc.) - Pension rights - Vulnerable male employment

Characteristics: - Zero slope for many - Rights already uniformly granted

##### Interpretation:

Indicators that reached full saturation before 2000; do not change over time.

### 8. Cluster 7 – “Labor Market Stress & Cyclical Unemployment”

Indicators: - Female unemployment - Male unemployment

Characteristics: - Moderate volatility - Clear business cycle response - Sensitive to conflict & economic recessions

##### Interpretation:

Labor market stress indicators showing cyclical and crisis-driven fluctuations.

## 5. Cluster Summary Table

|**Cluster ID**|**Cluster Name**|**Description**|**Example Indicators**|
|---|---|---|---|
|**0**|**Gradual Structural Demographic Shifts**|Slow-moving, long-term demographic & labor indicators with moderate volatility and minimal event sensitivity.|Life expectancy (M/F), Birth rate, Employment-to-population ratio|
|**1**|**Highly Volatile, Reform-Sensitive Indicators**|Strongly affected by reforms, crises, and policy cycles; high spikes/dips and large variance.|Inflation, WBL indicators (Mobility, Pay, Marriage, Entrepreneurship), Seats held by women, Education GPI|
|**2**|**Scale Outlier: Population Dynamics**|Unique large-scale behavior; mathematically incomparable to other indicators.|Total population|
|**3**|**Stable Legal Rights (One-Time Reforms)**|Binary legal rights that change only when laws pass; extremely stable over time.|Passport rights, Mobility inside country, Industrial job rights, Divorce rights, Domestic violence legislation|
|**4**|**Declining Health & Fertility Indicators**|Strong negative long-term slopes; structural demographic transitions.|Maternal mortality, Adolescent fertility, Crude death rate|
|**5**|**Growing Participation & Economic Capacity Indicators**|Strong positive trends, moderate volatility; reflect economic cycles + reform progress.|GDP per capita, Women in parliament, WBL Parenthood & Workplace indices|
|**6**|**Fully Stabilized Legal Rights**|Rights already granted across all years; almost zero slope & volatility.|Right to open bank account, Sign contract, Register business, Remarriage rights|
|**7**|**Labor Market Stress & Cyclical Unemployment**|Sensitive to economic conditions, conflict, and recessions; moderate volatility.|Female unemployment, Male unemployment|

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

✔ Event overlay analysis ✔ Risk-of-regression classification ✔ Tableau storytelling (indicator cluster filters) ✔ Thesis chapters on shock sensitivity and resilience