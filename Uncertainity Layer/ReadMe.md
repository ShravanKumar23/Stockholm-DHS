# Uncertainty Framework for Energy System Modelling

This repository contains the workflow for generating stochastic scenarios, running them in an energy system model, and post-processing results using clustering techniques.

The framework is designed to support uncertainty-aware planning of energy systems, particularly for applications such as district heating and excess heat integration.

---

## Overview

The workflow consists of three main steps:

1. Scenario generation under uncertainty  
2. Model execution using OSeMOSYS  
3. Clustering and analysis of results  

This structure allows exploration of a wide range of possible futures and identification of robust system configurations.

---

## 1. Scenario Generation

The first step is to generate scenarios that capture key uncertainties in the system.

### Key Features

- Sampling-based scenario creation  
- Representation of multiple uncertainty dimensions  
- Temporal consistency in generated data  

### Types of Uncertainty

The framework supports multiple sources of uncertainty, for example:

- Technology performance (e.g., COP variations)  
- Resource availability  
- Demand fluctuations  
- External shocks (e.g., extreme events)  

### Methods Used

- Latin Hypercube Sampling (LHS) for structured sampling  
- Stochastic processes (Poisson-based) for temporal variation  
- Parameter distributions (uniform, normal, etc.) depending on data availability  

The output of this step is a set of structured scenario inputs compatible with energy system models.

---

## 2. Running the Model (OSeMOSYS)

Once scenarios are generated, they are used as inputs to the energy system model.

### Model Used

- OSeMOSYS (Open Source Energy Modelling System)

OSeMOSYS is a long-term optimisation model that determines cost-optimal investment and operation strategies for energy systems.

### Execution Steps

1. Prepare input data for each scenario  
2. Run OSeMOSYS for all scenarios  
3. Store outputs including:
   - Investment decisions  
   - Technology capacities  
   - Energy flows  
   - System costs  

Each scenario results in one full model run.

---

## 3. Clustering and Analysis

After running all scenarios, the results are analysed to identify patterns and robust strategies.

### Purpose

- Reduce dimensionality of large scenario sets  
- Identify representative pathways  
- Detect robust and extreme system configurations  

### Methods

- DTW clustering 
- Distance metrics based on:
  - Technology installed capacity

### Outputs

- Clustered scenario groups  
- Representative (centroid) scenarios  
- Identification of:
  - Key drivers
  - Technology level Robustness 
  - Cross technology perason matrix

---

## Workflow Summary
