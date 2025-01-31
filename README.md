# Stockholm DHS Optimization Framework

## Overview

This repository provides a framework for the **spatio-temporal, technical, and economic optimization** of **Urban Excess Heat (UEH)** integration into **District Heating Systems (DHS)**. The framework uses a **multi-stage** optimization methodology to model the integration of UEH sources, optimize long-term investments, and evaluate operational feasibility. It consists of three key stages:

1. **Geospatial Optimization**
2. **Long-Term Investment Planning with OSeMOSYS**
3. **Operational Feasibility via Short-Term Dispatch Analysis**

The framework is applied to Stockholm’s **District Heating Network (DHN)**, which is one of the largest district heating systems in the world, with over 80% of buildings connected. The integration of UEH into the DHN is evaluated considering the spatial distribution of heat sources, the techno-economic feasibility of the infrastructure, and the long-term and operational performance of the system.

## Methodology
![image](https://github.com/user-attachments/assets/2888485f-3fc6-4f19-a147-85395d07175a)


### Stage 1 – Geospatial Optimization

In the first stage, the **geospatial mapping** of both **UEH sources** and the **DHN** is carried out. The spatial data, including the locations of existing DH pipelines and potential UEH sources (such as **data centers**, **supermarkets**, and **industrial waste heat**), are sourced from literature and publicly available databases (e.g., Open Street Maps). 

Using a **geospatial optimization tool**, the **costs** and **network losses** associated with connecting UEH sources to the DHN are calculated. The tool identifies the nearest connection points on the DHN, and it computes the **minimum-cost** network extensions, factoring in terrain, infrastructure, and road networks.

- **Key Outputs**: 
  - Network connection costs in **SEK/kW**
  - Network losses in **kW**
  - Geospatial location of the nearest DHN points for each UEH source

### Stage 2 – Long-Term Investment Planning with OSeMOSYS

In the second stage, the results from Stage 1 (i.e., network connection costs and losses) are input into the **OSeMOSYS** optimization model. OSeMOSYS is an open-source **energy system optimization tool** that optimizes investments in **technologies** and **grid extensions** over a long-term planning horizon (typically 30 years).

The model optimizes the integration of **UEH** sources alongside other technologies (e.g., **biomass**, **natural gas**, **solar thermal**) to determine the **cost-optimal** long-term energy system. It accounts for both **capital costs** and **operational costs** associated with UEH integration into the DHS.

- **Key Outputs**: 
  - **Cost-optimal capacities** of various UEH sources
  - **Annual heat recovered** from each UEH source
  - **Investment strategies** and **operational plans** for the DHS

### Stage 3 – Operational Feasibility via Short-Term Dispatch Analysis

The third stage involves **short-term dispatch optimization** to assess the **operational feasibility** of the long-term plan derived in Stage 2. The **dispatch model** operates at **higher temporal resolution** (e.g., hourly) to evaluate the **daily and seasonal fluctuations** of UEH sources. The model considers variations in the heat supply and demand, ensuring that the **cost-optimal capacities** are feasible for daily operations.

If discrepancies are found between the long-term optimization and operational feasibility (e.g., insufficient capacity during peak demand periods), the model re-optimizes the system with adjusted capacities and additional time steps, considering **peak demand** and **seasonal variations**.

- **Key Outputs**:
  - **Operational feasibility** of the long-term plan
  - **Intra-annual operation** of the DHS and UEH technologies
  - Adjustments to capacity dimensions to meet peak demand

## Features

- **Geospatial Optimization**: Efficiently calculates the most cost-effective UEH network connection points using spatial data.
- **Long-Term Investment Planning**: Optimizes investments in technologies and grid extensions to incorporate UEH into the DHS using OSeMOSYS.
- **Operational Feasibility Analysis**: Evaluates the real-time feasibility of the optimized system using a high-resolution dispatch model.
- **Flexible Framework**: The framework can be applied to other cities or regions with district heating systems and varying UEH sources.
- **Data Integration**: Allows for integration with a variety of geospatial and energy data sources, such as **Open Street Maps** and **OSeMOSYS-compatible datasets**.

## Prerequisites

To use this framework, the following tools are required:

- **Python 3.x**: For running the geospatial optimization and dispatch models.
- **OSeMOSYS**: For long-term energy system optimization.
- **Open Street Maps**: For geospatial data.
- **Pyomo**: For mathematical optimization.
- **Other Python Libraries**:
  - **pandas**
  - **geopandas**
  - **numpy**
  - **matplotlib**
- The environments used for each tools are available within each folder.
## Installation

1. Clone the repository:

   ```bash
   git clone https://github.com/ShravanKumar23/Stockholm-DHS.git
   cd Stockholm-DHS
   ```

2. Install the required conda environemtns from the yml files:

   ```bash
   conda env create -f environment.yml
   ```

3. Set up each tool by installing the required conda envionments.
## Usage

To run the full optimization framework, use the following steps:

1. **Prepare your data**: Ensure that all required geospatial and energy system data (e.g., UEH source locations, DHN map, technology costs) are available. To find the nearest point for each source on the network, use the'nearest point.py'. Add the polygons for each source based on the locations. Finalise the data for each source, i.e. coordinates and the polygons according to the format provided in the files in Input_Data. 
3. **Run the Geospatial Optimization Tool**: This tool will calculate the **costs** and **network losses** for connecting each UEH source to the DHS. Make sure that you have selected the appropriate input file from the Input_Data folder in the run.py file.
   ```bash
   conda activate emb3rs-gis-module
   python run.py
   ```
4. **Run the OSeMOSYS Model**: Input the results from the geospatial optimization to determine the **cost-optimal** long-term investment plan. The input file for the OSeMOSYS model needs to be prepared. Refer to structure and instructions [here]([url](https://github.com/OSeMOSYS/OSeMOSYS_PuLP)) and [here]([url](https://osemosys.readthedocs.io/en/latest/)). The ouput format can be csv or excel. This version of OSemOSYS supports several solver, cplex, gurobi, cbc, glpk, and scip.
 ```bash
   conda activate OSeMOSYS
   python OSeMOSYS.py -i inputfile.xlsx -s solver -o output format
   ```
5. **Run the Dispatch Model**: Input the optimized capacities from OSeMOSYS to analyze the **operational feasibility** at a high temporal resolution. The required inputs for the dispatch models are presented in Sample_input.xslx. Prepare the inputs.xlsx according to the instructions [here]([url](https://hotmapsdispatch.readthedocs.io/en/latest/Getting%20Started.html#user-guide))
```bash
   conda activate hotmapsDispatch
   ```
```bash
   python readWriteDatFiles.py
   ```
```bash
   python generateinputs.py
   ```
```bash
   cd hotmapsDispatch
   ```
```bash
   python -m app --s solver --n 1
   ```
7. Review the outputs from each stage to assess the feasibility and optimal pathways for integrating UEH into the DHS.
## Contributing

We welcome contributions to this repository. If you have suggestions or improvements, feel free to open an issue or create a pull request. Please ensure that you follow the project’s coding standards and provide clear documentation for any new features or changes.

## License

This project is licensed under the Apache 2.0 – see the [LICENSE](LICENSE) file for details.
