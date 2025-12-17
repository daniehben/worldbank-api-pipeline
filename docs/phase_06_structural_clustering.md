# ♦︎ Phase 6 – PCA-Based Clustering Analysis
Location: notebooks/machine_L.ipynb

## Purpose

This phase used the PCA-transformed feature space to uncover structural groupings among MENA countries. By combining dimensionality reduction with clustering algorithms, the goal was to identify shared gender-development patterns and build country archetypes that can later be used for storytelling, dashboard segmentation, and policy interpretation.

## 1. Preparing PCA Output for Clustering

After PCA reduced the dataset to three core components (PC1–PC3), these components were used as the feature space for clustering.

Steps performed:

- Extracted PC1, PC2, and PC3 scores for each country
    
- Merged PCA output with country and region labels
    
- Ensured no missing values, consistent indexing, and proper scaling
    
- Confirmed that PCA space captures ~69% of total variance → sufficient for clustering structure
    

### Why PCA space?

It removes noise, minimizes multicollinearity, and ensures that clustering is driven by meaningful latent dimensions, not raw indicator scale differences.


## 2. Determining Optimal Number of Clusters

Multiple validation metrics were computed across k = 2 → 8:

- Elbow (Inertia)
    
- Silhouette Score
    
- Davies–Bouldin Index
    
- Calinski–Harabasz Index
    

Across these metrics, k = 5 consistently provided the best balance between cohesion and separation.

Silhouette ≈ 0.39 → moderate separation, acceptable given the small dataset and strong regional heterogeneity.

A combined elbow–silhouette plot was saved as documentation.

## 3. Fitting K-Means on PCA Components

A K-Means model with k = 5 was fitted using PC1–PC3 as inputs.

Core logic:

- Countries positioned close in PCA space were grouped based on shared gender, economic, and demographic characteristics
    
- Cluster labels were added back into the ML dataset for profiling
    
- Cluster sizes were examined to ensure stability
    

This produced five interpretable archetypes.

## 4. Cluster Membership (Final Assignments)

The model yielded the following groupings:

|Cluster|Countries|Interpretation|
|---|---|---|
|**0**|Lebanon|Unique parity + inflation instability profile|
|**1**|Bahrain, Kuwait, Oman, Qatar, Saudi Arabia, UAE|High-income Gulf economies with strong economic empowerment but low political representation|
|**2**|Egypt, Iran, Iraq, Jordan, Syria, Yemen|High demographic pressure, high maternal mortality, low economic empowerment|
|**3**|Algeria, Libya, Morocco, Tunisia|North African reformist bloc with stronger laws and moderate outcomes|
|**4**|West Bank & Gaza|An institutional/political outlier with atypically high representation metrics|

## 5. Cluster Profile Construction

To interpret the clusters, we calculated the mean value of each selected feature within each group.

Indicators included:

- GDP per capita
- Women in parliament (%)
- WBL Index
- Education GPI
- Maternal mortality
- Adolescent fertility
- Female employment rate
- Inflation
- Population

Because raw values differ drastically in scale, all features were normalized (z-scores) before visualization.

## 6. Normalized Radar Charts (Cluster Personality Maps)

For each cluster, a radar chart was generated showing its standardized feature profile.

These charts reveal each group’s strengths, weaknesses, and structural signatures, such as:

- Cluster 1 (GCC): high GDP, high female employment, low adolescent fertility, low political representation
    
- Cluster 3 (North Africa): high WBL and education parity, moderate political voice, mixed economic strength
    
- Cluster 2 (Fragile States): extreme demographic pressure and health burden, low empowerment
    
- Cluster 0 (Lebanon): strong institutional/education indicators contrasted with economic instability
    
- Cluster 4 (West Bank): exceptional political representation relative to economic baseline
    

These profiles will feed directly into Tableau dashboards and thesis narrative sections.
