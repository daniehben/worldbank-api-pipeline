# ♦︎ Phase 7 – Trajectory-Based Country Clustering

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
    
    - cat__trend_slope
    - Mean of trend_slope across all indicators in that category
2. Average annual growth
    
    - cat__cagr
    - Captures sustained growth/decline rather than just slope units
3. 5-year volatility
    
    - cat__rolling_std_5y
    - Measures instability of the category over time
4. Momentum (recent vs long-term)
    
    - cat__momentum
    - Positive = recent acceleration, negative = recent slowdown/regression
5. Event sensitivity
    
    - cat__spike – average spike count
    - cat__dip – average dip count
6. Relative dispersion
    
    - cat__coef_variation – normalized variability

This helped reduce noise from individual indicators while preserving thematic structure. It speaks the language of your storyline: “Is governance becoming more volatile? Are legal reforms accelerating? Are demographic indicators stabilizing?” It keeps the feature space manageable (~60 country-category metrics) and interpretable.

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

The trajectory features were standardised as there are multiple scales (e.g., spike counts vs. slope units vs. volatility). PCA and K-Means are distance-based; without scaling, large-scale features would dominate. Z-scores make each feature contribute equally.

### 3.2 Trajectory PCA

PCA was visualised via:

- Scree plot (variance explained per component)
- Cumulative variance plot (how much total structure is captured with k PCs)

These plots revealed -> PC1_traj ≈ 21.8%, PC2_traj ≈ 13.2%, PC3_traj ~ 10–11% Thus by ~6 components, you capture ~70–75% of total variance

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
![[Pasted image 20251217190553.png]]

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
    - Slightly negative or flat in Demographics, Governance, WBL → Countries improve gradually in socio-economic and labour outcomes without dramatic institutional shifts.
2. Cluster 1 – Education-Led Social Improvers
    
    - Strong positive slope in Education and Demographics
    - Health modestly improving
    - Labor, Governance, LegalAutonomy more muted → These countries are pulled forward by schooling and demographic transitions rather than legal or economic reforms.
3. Cluster 2 – Fragility-Driven Decliners
    
    - Negative or flat slopes in Health and Labor
    - Governance slightly positive (likely weak recovery after collapse)
    - WBL and LegalAutonomy positive but modest → This is the “fragile recovery / stop–go progress” cluster: some legal progress amid socio-economic deterioration.
4. Cluster 3 – High-Volatility Reform Accelerators
    
    - Strong positive slopes in WBL, LegalAutonomy, Labor, Health
    - Economy modestly positive → These states are pushing structural reforms in labour and legal rights, and health systems are improving strongly.
5. Cluster 4 – Governance-Unstable Shock States
    
    - Dominant positive slope in Governance
    - Mixed/flat elsewhere → On average, they show recovery or restructuring in governance, but other categories lag or move unevenly.
6. Cluster 5 – Stable High-Capacity Reformers
    
    - Positive slopes across almost all categories, especially Economy, WBL, LegalAutonomy, Labor, Health → These are broad-based improvers with systemic reforms across economic, legal, and social dimensions.

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
- 