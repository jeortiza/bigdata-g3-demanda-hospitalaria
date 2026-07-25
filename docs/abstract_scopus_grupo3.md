# Draft Paper — Scopus Abstract

**Course:** Big Data DD283 (2026-1) · Universidad Autónoma del Perú
**Group 3 — Junior Ortiz**
**Project:** Hospital Demand Forecasting System — EsSalud

---

## Title

**Hospital Demand Forecasting for EsSalud Peru: A Medallion Big Data Architecture Integrating Climate and Epidemiological Signals**

---

## Abstract (249 words)

Hospital overcrowding during seasonal disease peaks strains healthcare systems in Lima, Peru, where dengue outbreaks and influenza waves drive sharp, hard-to-anticipate demand surges. This study develops an end-to-end Big Data platform to forecast weekly hospital demand four weeks ahead for EsSalud, integrating administrative attention records with climatic and epidemiological sources. A synthetic dataset of 500,000 attentions across three hospitals (2022–2024) was processed through a PySpark Medallion architecture (Bronze–Silver–Gold), storing analytical tables in Parquet and MongoDB Atlas. Demand was modeled with Facebook Prophet using multiplicative seasonality and four exogenous regressors (lagged temperature, lagged dengue cases, school-start and holiday indicators). Cross-validated forecasts achieved a Mean Absolute Percentage Error below 15% for all four specialties (3.6%–12.5%). Outbreak detection via Isolation Forest identified six epidemic weeks. Eight Great Expectations rules validated data quality across 500,000 records with full integrity of the ISO-8601 epidemiological week key. The synthetic generator was contrasted against real CDC/RENACE dengue surveillance for Lima, revealing that it reproduces the climate–dengue correlation (r=0.90) but not the real epidemic dynamics (94-fold versus 1-fold interannual growth), a limitation documented transparently. Results feed an executive Streamlit dashboard with eight management KPIs and three role-based views, including a geospatial map of projected demand. The work demonstrates a reproducible, auditable pipeline whose main methodological contribution is the systematic verification of data veracity across heterogeneous health sources.

**Keywords:** hospital demand forecasting; Prophet; Medallion architecture; epidemiological surveillance; data quality; Big Data healthcare

---

## Methodology (extended)

**Data architecture.** The pipeline follows a Medallion design implemented in PySpark. The Bronze layer ingests four raw sources (hospital attentions, Lima climate, MINSA/CDC epidemiology, and hospital supply). The Silver layer standardizes schemas, normalizes CIE-10 codes, derives age groups and 7/14/21-day lag features, and builds the ISO-8601 epidemiological week key. The Gold layer produces four analytical tables consumed by the forecasting models and the executive dashboard.

**Forecasting.** Weekly demand series (156 weeks; 155 after lag construction) were modeled per specialty with Facebook Prophet. Multiplicative seasonality was adopted after comparing additive and multiplicative modes by rolling-origin cross-validation (12 folds, 4-week horizon). Four exogenous regressors from the project hypotheses were included. Model skill was reported as cross-validated MAPE.

**Outbreak detection.** Weeks with anomalous dengue activity were flagged with Isolation Forest over standardized climate–epidemiology features. The structural gap between an unsupervised anomaly detector and a univariate threshold label was analyzed and documented.

**Data quality.** Eight Great Expectations validations covered uniqueness, schema, completeness, domain, range, format, consistency, and temporal integrity, the last one independently reconstructing the ISO week key from the attention date.

**External validation.** The synthetic dengue series was benchmarked against real CDC/RENACE surveillance for Lima (119,537 notified cases, 2022–2024), quantifying where the generator matches reality (seasonal relationship) and where it diverges (epidemic magnitude and peak timing).

**Presentation.** An executive Streamlit dashboard delivers eight management KPIs and three role-based views (Hospital Director, Network Manager, Epidemiologist), including a geospatial map of projected demand across the three establishments.
