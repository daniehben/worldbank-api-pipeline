# ♦︎ Phase 5 – ML Prep and Principal Component Analysis

Location: notebooks/mL_prep.ipynb

## Purpose

This phase prepared the cleaned dataset for machine-learning analysis by (1) selecting a statistically sound feature subset using a correlation matrix, and (2) applying PCA to uncover the core latent dimensions that drive gender-development differences across MENA countries.

## 1. Feature Selection Using Correlation Matrix

The initial dataset contained 60+ engineered indicators. Before modeling, we performed feature reduction to prevent multicollinearity, over-weighting, and the curse of dimensionality.

#### Process

- Computed a full correlation matrix on high-variance indicators
    
- Identified tight clusters (GDP, population, WBL sub-indices, ministerial/parliament indicators)
    
- Removed redundant indicators showing correlation > 0.90
    
- Retained only structurally unique, high-signal features
    

#### Final Feature Set

Nine indicators capturing the core gender-development dimensions:

1. Female labor force participation (%)
    
2. GDP per capita (US$)
    
3. Women in Parliament (%)
    
4. WBL Index Score
    
5. Education GPI (secondary)
    
6. Maternal mortality ratio
    
7. Adolescent fertility rate
    
8. Population (total)
    
9. Inflation

## 2. Scaling & Standardization

All features were standardized (z-score) to remove unit differences (%, per 100k, index values, USD). Standardization ensures that PCA treats each indicator equally.

## 3. PCA Fitting & Variance Reduction

A full PCA model was fitted on the scaled feature matrix.

Variance Structure

- PC1: 36.2%
    
- PC2: 18.6%
    
- PC3: 14.1%
    

→ Total explained by PC1–PC3: ~69%

The scree plot showed a strong elbow, confirming meaningful underlying structure. 


## 4. PCA Component Interpretation

#### PC1 — Structural Gender Development

Captures a development gradient driven by:

- GDP per capita, + female labor participation, − adolescent fertility, − maternal mortality. → Represents economic empowerment & health advancement.

#### PC2 — Legal Equality vs Political Representation
- High: WBL score, education parity
- Low: women in parliament → Represents institutional reforms vs actual political voice.

#### PC3 — Economic Instability & Uneven Gender Outcomes

- Mixed signals involving inflation, education, WBL, and health indicators. → Represents policy–outcome inconsistency under economic pressure.

## 5. PCA Country Distribution

The PC1 × PC2 scatter shows distinct regional patterns:

- GCC: high PC1 (strong development/economic empowerment)
    
- North Africa: mid-PC1, high-PC2 (strong laws/education vs weaker representation)
    
- Fragile States: low PC1 (health + demographic burden)
    
- Lebanon: PC2 outlier (high legal/education indicators but weak political structures)
