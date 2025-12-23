# 🌐 MENA Gender Data Dashboard  
#### End-to-End Gender Development Analysis (2000–2023)


## ⦿ Overview
This project is an end-to-end analytical pipeline that examines gender-related development patterns across Middle East and North Africa (MENA) countries using World Bank and UNESCO data. It integrates automated data collection, rigorous data cleaning, feature engineering, machine learning, and event-based analysis to produce interpretable, policy-relevant insights.

Rather than focusing solely on point-in-time comparisons, the project emphasizes **trajectories, volatility, shock sensitivity, and structural differences** across countries and indicators.

## ⦿ Project Purpose
The project aims to answer the following core questions:

- How do MENA countries differ structurally in gender-related development?
- Which countries improve steadily, which reform in bursts, and which regress under shocks?
- Which indicators are stable, volatile, or crisis-sensitive?
- How do political, economic, and social events shape observed data behavior?

The analysis prioritizes **interpretability, reproducibility, and narrative coherence** over purely predictive performance.

---

## ⦿ Data Sources
- **World Bank Gender Statistics & Development Indicators**
- **UNESCO Institute for Statistics (Education & Literacy)**

Coverage:
- Time span: 2000–2023  
- Countries: MENA region (including GCC, Levant, North Africa, and fragile states)  
- Indicators: Gender, legal rights, education, health, labor, demographics, and economic measures  

---

## ⦿ Repository Structure
```
.
├── src/
│   ├── api_fetcher.py        
│   ├── unit_types.py
│   
│ 
├── analysis/
│   ├── analysis_outputs/             
│   ├── methodological_caveats/
│   ├── synthesis/
│                  
│   
├── country_reports/       
│
├── notebooks/
│   ├── anomaly.ipynb
│   ├── cleaning.ipynb
│   ├── country_indicator.ipynb
│   ├── data_merge.ipynb
│   ├── feature_eng.ipynb
│   ├── mL_prep.ipynb
│   ├── clustering.ipynb
│   ├── traj_clustering.ipynb
│   ├── indicator_clustering.ipynb
│   └── event_overlay.ipynb
│
├── data/
│ 
│
├── docs/
│   ├── phase_01_data_collection.md
│   ├── phase_02_external_data.md
│   ├── phase_03_cleaning_validation.md
│   ├── phase_04_feature_engineering.md
│   ├── phase_05_pca_analysis.md
│   ├── phase_06_structural_clustering.md
│   ├── phase_07_trajectory_clustering.md
│   ├── phase_08_indicator_behavior.md
│   ├── phase_09_event_analysis.md
│   ├── phase_10.1_pre_classification_prep.md
│   ├── phase_10.2_classification_problem.md
│   ├── phase_10.3_classification.md
│   ├── phase_10.4_SHAP_explainability.md
│   ├── phase_10.5_SHAP_temporal_robustness.md
│   ├── phase_10.6_SHAP_seed_robustness.md
│   ├── phase_10.7_SHAP_capacity_robustness.md
│   ├── phase_10.3_classification.md
│   ├── phase_10.4_SHAP_explainability.md
│   ├── phase_11_cross_model_synthesis.md
│
├── outputs/
│   ├── charts/             
│   ├── country_narratives/
│   ├── country_reports/
│   ├── cross_models/
│   ├── csv_files/              
│   └── md_files/      
│
└── README.md
```

Each analytical phase is documented separately under `docs/` to keep this README concise.

---

## ⦿ Analytical Pipeline (High-Level)
The project follows a modular pipeline:

1. Automated data collection & harmonization  
2. Missingness-aware cleaning & validation  
3. Feature engineering (trends, volatility, momentum, shocks)  
4. Machine learning & statistical modeling  
5. Event overlay & shock sensitivity analysis  
6. Cross-model integration & synthesis  


A concise phase overview is provided below. Full technical details are available in the documentation.

---

## ⦿ Pipeline Overview by Phase

| Phase | Focus |
|------|------|
| Phase 1 | Automated data collection (World Bank, UNESCO) |
| Phase 2 | External data integration & harmonization |
| Phase 3 | Data cleaning & missingness validation |
| Phase 4 | Feature engineering & ML dataset construction |
| Phase 5 | Structural clustering & PCA |
| Phase 6 | Trajectory-based country clustering |
| Phase 7 | Indicator behavior clustering |
| Phase 8 | Event overlay & shock sensitivity analysis |
| Phase 9 | Cross-model integration & synthesis |

---


## ⦿ Key Outputs & Deliverables
- Cleaned and feature-engineered ML-ready dataset  
- Structural country clusters and trajectory-based typologies  
- Indicator-level behavioral classifications  
- Event-aligned shock sensitivity analysis  
- Cross-model integration framework linking structure, movement, risk, and events  
- Static analytical reports and visualizations suitable for research or portfolio use  

---

## ⦿ How to Use This Project

### 1. Orientation
- Start with this README for the conceptual overview.
- Review `/docs/` for detailed phase-by-phase explanations.

### 2. Reproducing the Analysis
- Run notebooks sequentially if reproducing the full pipeline.
- Most notebooks can also be explored independently once datasets are generated.

### 3. Understanding Results
- Key findings are documented in:
  - `docs/phase_*` files  
  - `outputs/` (figures, country reports, summary tables)

### 4. Extending the Project
- New indicators or countries can be added via the API-fetching module.
- Additional models (e.g., alternative classifiers or dimensionality reduction methods) can be integrated using the existing feature set.

---

## ⦿ Documentation
All methodological decisions, assumptions, and analytical interpretations are documented in the `/docs` directory. Each phase builds on the previous one and can be reviewed independently.

---

## ⚠️ **Data Coverage Sensitivity**
All analytical results should be interpreted in light of reporting reliability and statistical capacity.
See: `/analysis/data_coverage_sensitivity.md`


## ⦿ Scope & Notes
- This project prioritizes **research-grade analysis** and interpretability.
- Interactive dashboards are optional; all findings are supported by static, reproducible outputs.
- The repository is suitable for policy analysis, academic work, and professional portfolios.

---

## ⦿ License
This project is intended for educational and research purposes.





