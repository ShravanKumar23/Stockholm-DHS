from pathlib import Path
import pandas as pd
from openpyxl import load_workbook
import os
import zipfile

BASE_DIR = Path(__file__).parent.resolve()
inputs_path = BASE_DIR.joinpath("Inputs.xlsx")
default_input_path = BASE_DIR.joinpath("Sample_Input.xlsx")


def read_input(sheetname: str, path2input=inputs_path):
    return pd.read_excel(path2input, sheet_name=sheetname)


def update_inputs(inputs_path, year, price_scen, temperature_scen, el_taxes_fees):
    heat_generators = read_input(sheetname="Heat Generators")
    capital_costs = read_input(sheetname="capital cost table")
    fixed_costs = read_input(sheetname="fixed cost table")
    storage_params = read_input(sheetname="Heat Storage")
    prices_emission = read_input(sheetname="prices and emmision factors")
    el_emission = read_input(sheetname="Emission Factor for electricity")
    fuel_prices = read_input(sheetname="Fuel Prices")
    other_data = read_input(sheetname="Data")
    installed_capacities = read_input(sheetname="installed capacities")
    variable_costs = read_input(sheetname="variable costs")
    co2_price = read_input(sheetname="co2 price")
    demand = read_input(sheetname="demand")
    maxavailability = read_input(sheetname="Maximal Available Potential")

    default_input = load_workbook(default_input_path)
    default_input_data = pd.ExcelFile(default_input_path)

    # Dictionary to hold the dataframes for each sheet
    sheets_dict = {}

    # Read each sheet into a dataframe
    for sheet_name in default_input_data.sheet_names:
        sheets_dict[sheet_name] = pd.read_excel(
            default_input_data, sheet_name=sheet_name
        )

    # Update the dataframes with the new data
    sheets_dict["Heat Generators"] = pd.concat(
        [
            sheets_dict["Heat Generators"],
            pd.DataFrame(
                heat_generators.values, columns=sheets_dict["Heat Generators"].columns
            ),
        ],
        ignore_index=True,
    )

    # update installed capacities
    for i in heat_generators.name.values:
        try:
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i, ["installed capacity (MW_th)"]
            ] = installed_capacities.loc[(installed_capacities["name"] == i) & (installed_capacities["Temperature"] == temperature_scen) & (installed_capacities["Price"] == price_scen), year].values[0]
        except:
            print(f"Could not find {i} in heat_generators")
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i, ["installed capacity (MW_th)"]
            ] = None

    # Update the capital costs
    for i in sheets_dict["Heat Generators"].name.values:
        try:
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i,
                ["investment costs (EUR/MW_th)"],
            ] = (
                capital_costs.loc[capital_costs["Technology/Year"] == i, year].values[0]
                * 1e6
            )
        except:
            print(f"Could not find {i} in capital_costs")
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i,
                ["investment costs (EUR/MW_th)"],
            ] = None

    # Update the max potential
    for i in sheets_dict["Heat Generators"].name.values:
        try:
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i,
                ["Maximal Available Potential [MWh]"],
            ] = (
                maxavailability.loc[maxavailability["Technology/Year"] == i, year].values[0]
            )
        except:
            print(f"Could not find {i} in capital_costs")
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i,
                ["investment costs (EUR/MW_th)"],
            ] = None   

    # Update the fixed costs
    for i in sheets_dict["Heat Generators"].name.values:
        try:
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i, ["OPEX fix (EUR/MWa)"]
            ] = (
                fixed_costs.loc[fixed_costs["Technology/Year"] == i, year].values[0]
                * 1e6
            )
        except:
            print(f"Could not find {i} in fixed_costs")
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i, ["OPEX fix (EUR/MWa)"]
            ] = None

    # update the variable costs
    for i in sheets_dict["Heat Generators"].name.values:
        try:
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i, ["OPEX var (EUR/MWh)"]
            ] = variable_costs.loc[variable_costs["Technology/Year"] == i, year].values[
                0
            ]
        except:
            print(f"Could not find {i} in fixed_costs")
            sheets_dict["Heat Generators"].loc[
                sheets_dict["Heat Generators"].name == i, ["OPEX var (EUR/MWh)"]
            ] = None

    # Update the storage parameters
    sheets_dict["Heat Storage"].iloc[:, 1:] = (
        storage_params[storage_params.Year == year].iloc[:, 1:].values
    )

    # Update the prices and emission factors
    for ec in ["bio oil", "wood chips", "waste", "fine bio oil", "bio waste"]:
        try:
            sheets_dict["prices and emmision factors"].loc[
                sheets_dict["prices and emmision factors"]["energy carrier"] == ec,
                ["prices(EUR/MWh)"],
            ] = fuel_prices.loc[fuel_prices["Year"] == year, ec].values[0]
        except:
            print(f"Could not find {ec} in fuel_prices")
            sheets_dict["prices and emmision factors"].loc[
                sheets_dict["prices and emmision factors"]["energy carrier"] == ec,
                ["prices(EUR/MWh)"],
            ] = None

    # Update the emission factor for electricity
    sheets_dict["prices and emmision factors"].loc[
        sheets_dict["prices and emmision factors"]["energy carrier"] == "electricity",
        "emission factor [tCO2/MWh]",
    ] = el_emission.loc[
        el_emission["Year"] == year, "Emission Factor (Ton/MWh)"
    ].values[
        0
    ]

    # Update CO2 price
    sheets_dict["Data"]["CO2 Price [EUR/tC02]"] = co2_price.loc[
        co2_price["Year"] == year, "CO2 price (SEK/tCO2)"
    ].values[0]

    # update demand
    sheets_dict["Data"]["Total Demand[ MWh]"] = (
        demand.loc[demand["Year"] == year, "demand (GWh)"].values[0] * 1e3
    )

    # update profiles
    sheets_dict["Default - External Data"]["Radiation"] = "default # 2016"
    sheets_dict["Default - External Data"]["Temperature"] = "Sthlm # 0"
    # spot price
    sheets_dict["Default - External Data"][
        "Sale Electricity price"
    ] = f"{price_scen.lower()} # {year}"
    # purchase price for heat pumps --> need to add taxes and fees
    sheets_dict["Default - External Data"][
        "Electricity price"
    ] = f"{price_scen.lower()} # {year},{el_taxes_fees}f"
    sheets_dict["Default - External Data"]["Heat Demand"] = "Sthlm # 0"

    # assign temperature scenarios
    if temperature_scen == "High":
        sheets_dict["Default - External Data"][
            "Inlet Temperature"
        ] = f"{temperature_scen.lower()} # 0"
        sheets_dict["Default - External Data"][
            "Return Temperature"
        ] = f"{temperature_scen.lower()} # 0"
    else:
        sheets_dict["Default - External Data"][
            "Inlet Temperature"
        ] = f"{temperature_scen.lower()} # {year}"
        sheets_dict["Default - External Data"][
            "Return Temperature"
        ] = f"{temperature_scen.lower()} # {year}"

    sheets_dict["Default - External Data"]["River Temperature"] = "supermarket # 0"
    sheets_dict["Default - External Data"]["Waste Water Temperature"] = "Sthlm # 0"
    sheets_dict["Default - External Data"]["Data Center Temperature"] = "datacenter # 0"
    sheets_dict["Default - External Data"]["Seawater Temperature"] = "Sthlm # 0"

    output_path = inputs_path.parent.joinpath(
        f"Inputs_{year}_{temperature_scen}Temp_{price_scen}Price.xlsx"
    )

    # Write the updated dataframes to the new excel file
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        for sheet_name, df_sheet in sheets_dict.items():
            df_sheet.to_excel(writer, sheet_name=sheet_name, index=False)


el_taxes_fees = (
    150  # define taxes and fees for electricity, to be added on top of spot price # TO BE DEFINED
)

years = [2030, 2035, 2040, 2045, 2050]
price_scenarios = ["Low", "High"]
temperature_scenarios = ["Low", "High"]

# years = [2030]
# price_scenarios = ["Low"]
# temperature_scenarios = ["Low"]

# Set the parent folder where all year folders will be stored
parent_folder = BASE_DIR.joinpath("inputs")
os.makedirs(
    parent_folder, exist_ok=True
)  # Create the parent folder if it doesn't exist

# Modify this part to create year-based folder structure and save files accordingly
for year in years:
    # Create a folder for the specific year inside the 'inputs' folder
    year_folder = parent_folder.joinpath(f"{year}")
    os.makedirs(
        year_folder, exist_ok=True
    )  # This will create the folder if it doesn't exist

    for price_scen in price_scenarios:
        for temperature_scen in temperature_scenarios:
            # Define the output path inside the year folder
            output_path = year_folder.joinpath(
                f"{temperature_scen}Temp_{price_scen}Price.xlsx"
            )

            # Call the update_inputs function to generate the input file in the respective folder
            update_inputs(
                output_path, year, price_scen, temperature_scen, el_taxes_fees
            )

# Now, create a zip file for the 'inputs' folder
zip_file_path = BASE_DIR.joinpath("inputs.zip")

# Zip the entire 'inputs' folder
with zipfile.ZipFile(zip_file_path, "w", zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk(parent_folder):
        for file in files:
            file_path = Path(root).joinpath(file)
            arcname = file_path.relative_to(
                parent_folder
            )  # Ensure relative paths inside the zip
            zipf.write(file_path, arcname)
