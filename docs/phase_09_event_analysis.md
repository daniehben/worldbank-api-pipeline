# ♦︎ Phase 9 – Event Overlay & Shock Sensitivity Analysis

Location: notebooks/Event_Overlay.ipynb 
Inputs: Fully cleaned + feature-engineered panel dataset (country–year–indicator) with spikes, dips, volatility metrics, trend metrics, and event flags.

## 1. Purpose & Link to the Main Storyline

This phase introduces the temporal and political dimension of the MENA indicator system:

“How do countries and indicators react when real-world events occur?” “Which countries are resilient, reactive, or shock-sensitive?” “Which thematic areas respond to crises, and which respond to reform windows?”

Up to Phase 9, these insights were found:

- Country similarity structures (country clustering)
- Indicator behavior types (indicator clustering)
- Long-term trajectories (trend slopes, CAGR, momentum)
- Volatility structure (rolling std, coefficient of variation)

Phase 10 integrates actual historical events into the analytical narrative, enabling the connection between quantitative volatility and political timelines, such as:

- Arab Spring (2011–2013)
- 2014 Oil Crisis
- Syrian War escalation
- Yemen Civil War
- Libyan conflict
- COVID-19 (2020–2021)
- National legal reforms (WBL reforms, constitutional changes)

This allows the project to answer core storyline questions:

1. Sub-Storyline C — Crisis & Reform Sensitivity
    
    - How do governance, health, labor, and legal indicators respond to systemic shocks?
    - Do crises produce dips, or do reforms produce spikes?
    - Are some categories structurally resistant to instability?
2. Sub-Storyline D — Divergence of National Trajectories
    
    - Why did some countries deteriorate sharply during conflict (Libya, Syria, Yemen) while others barely moved?
    - Are zero-movement countries (Egypt, Yemen in some indicators) experiencing data suppression, missingness, or political reporting constraints?

## 2. Country-Level Event Impact Summaries

For each country, the following was generated:

- Summary table of all indicators
- Annual spike/dip counts
- Event-year overlays
- Category-specific response charts
- Multi-page PDF report (final deliverable)

Each PDF now includes:

- Country name + cluster placement
- Clean tables (auto-fitted, wrapped text)
- Category-by-category plots
- Event-aligned spike/dip timelines

This produces the first fully interpretable narrative deliverable of the project.

## 3. New Analytical Layers Introduced in This Phase

### 3.1 Event Sensitivity Score (ESS)

```shell
ESS = (Spikes + Dips during event years) / Total indicators
```

This immediately revealed:

- GCC → reform-sensitive but crisis-resistant (spikes > dips)
- Egypt & Yemen → zero ESS for multiple categories → indicating non-reporting, missing annual updates, or political data stagnation, not genuine stability.

### 3.2 Category–Event Alignment Matrix

This directly answers:

##### “Which thematic areas respond to which event types?”

Example structural finding:

|Category|Arab Spring|Oil Crisis|COVID-19|Local Political Events|
|---|---|---|---|---|
|Health|dip-heavy|flat|dip-heavy|variable|
|Governance|flat|flat|spike-heavy|highly volatile|
|Labor|mixed|flat|mixed|dip|
|WBL Legal Rights|flat|minimal|spike-heavy|spike|

### 3.3 Event Typology Differentiation (Crisis vs Reform)

Using spikes/dips and categories:

Crises → dips in Health, Labor, Governance

Reforms → spikes in WBL indicators

Global shocks (COVID-19) create synchronized declines across all countries

Local conflicts produce extreme dips

This brings the conclustion that:

##### Indicators behave differently depending on whether the event is a crisis or a reform.

## 4. Key Findings & Emerging Narrative

This phase reveals fundamental structural differences between countries and categories:

### 4.1 Cross-Country Insight

- Libya, Syria, Yemen → collapse-type responses
- Lebanon → economic crisis volatility
- GCC → reform-driven spikes, minimal dips
- Egypt → unnaturally flat pattern → strong indicator of data stagnation
- Morocco, Tunisia, Jordan → moderate sensitivity with selective reform impacts

### 4.2 Category-Level Insight

- WBL → responds only to reforms, not crises
- Health → highest dip concentration
- Labor → chronically unstable
- Governance → mirrors national political trajectories

### 4.3 Event-Type Insight

- Arab Spring → mixed crisis/reform signal
- Oil Shock 2014 → broad economic deterioration
- COVID → universal health collapse
- Local conflicts → persistent, deep structural dips

## 5. Deliverables Produced in This Phase

### 1. Full Country PDF Reports

Each includes:

- Summary table
- Category breakdown
- Spike & dip visualization
- Event annotations

### 2. Global Event Summary Report

Contains:

- Event Sensitivity ranking
- Category–Event Alignment Matrix
- Crisis vs Reform typology table

## 6. How This Phase Advances the Main Thesis

It explains why the data behaves the way it does.

These questions can be now answered:

1. Why some countries diverge
2. Why certain indicators fluctuate
3. Why some clusters exist
4. Why spikes and dips occur

This turns descriptive analytics into causal political-economic storytelling.
