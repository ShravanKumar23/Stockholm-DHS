# =============================================================================
# needed modules
# =============================================================================
from pathlib import Path
from pprint import pprint
import pandas as pd
import numpy as np
import pickle
from matplotlib import pyplot as plt

# =============================================================================
# global paths and variables
# =============================================================================
BASE_DIR = Path(__file__).parent.resolve()
dat_path = BASE_DIR.joinpath("hotmapsDispatch/app/modules/common/AD/F16_input")


# =============================================================================
# functions and classes
# =============================================================================
def opendat(string, path2dat=dat_path):
    profile_path = path2dat.joinpath(f"{string}_profiles.dat")
    with open(profile_path, "rb") as file:
        profile = pickle.load(file)

    mapper_path = path2dat.joinpath(f"{string}_name_map.dat")
    with open(mapper_path, "rb") as file:
        mapper = pickle.load(file)

    return profile, mapper


# =============================================================================
def savedat(profile, mapper, string, path2dat=dat_path):
    profile_path = path2dat.joinpath(f"{string}_profiles.dat")
    with open(profile_path, "wb") as file:
        pickle.dump(profile, file)

    mapper_path = path2dat.joinpath(f"{string}_name_map.dat")
    with open(mapper_path, "wb") as file:
        pickle.dump(mapper, file)


# =============================================================================


def empty_dats(BASE_DIR):
    for fp in BASE_DIR.glob("*profiles.dat"):
        with open(fp, "wb") as f:
            pickle.dump({("default", 2016): np.ones(8760) * 1e-3}, f)
    for fp in BASE_DIR.glob("*map.dat"):
        with open(fp, "wb") as f:
            pickle.dump(dict(default="Wien"), f)
    return None


# =============================================================================


def interpolate_temperature(
    profile, temp1, temp2, temp3, temp4, load1, load2, load3, load4
):
    temp = []
    for i in range(len(profile)):
        if profile[i] <= load1:
            temp.append(0 + (temp1 - 0) * (profile[i] - 0) / (load1 - 0))
        elif (profile[i] > load1) & (profile[i] <= load2):
            temp.append(
                temp1 + (temp2 - temp1) * (profile[i] - load1) / (load2 - load1)
            )
        elif (profile[i] > load2) & (profile[i] <= load3):
            temp.append(
                temp2 + (temp3 - temp2) * (profile[i] - load2) / (load3 - load2)
            )
        elif (profile[i] > load3) & (profile[i] <= load4):
            temp.append(
                temp3 + (temp4 - temp3) * (profile[i] - load3) / (load4 - load3)
            )
    return temp


def add_profile(short, name, year, profile, string, new=True):
    """This function adds a numpy array into the dat files

    Args:
        short (str): short hand name of the profile
        name (str): long name of the profile
        year (int): year of the profile
        profile (numpy.array): (8760,) vector
        string (str): name of the dat file
                      options available:
                        "inlet_temperature",
                        "load",
                        "price",
                        "radiation",
                        "return_temperature",
                        "river_temperature",
                        "temperature",
                        "wastewater_temperature"
        new (bool, optional): flag to create a new dat file or to add .
                              Defaults to True (creates a new dat file with the profile).

    Returns:
        (bool): True if it succeed and False if it failed to add the profile
    """

    dat_files = [
        "inlet_temperature",
        "load",
        "price",
        "radiation",
        "return_temperature",
        "river_temperature",
        "temperature",
        "wastewater_temperature",
        "datacenter_temperature",
        "seawater_temperature",
    ]
    if string not in dat_files:
        print(f"{string} is not a valid dat file, select one of {dat_files}")
        return False
    if len(profile) != 8760:
        print(f"The profile you want to add is not 8760 long but {len(profile)} ")
        return False
    m = {short: name}
    p = {(short, int(year)): profile}
    if new:
        p2, m2 = {("default", 2016): np.ones(8760) * 1e-3}, dict(default="Wien")
    else:
        p2, m2 = opendat(string)
    m = {**m2, **m}
    # print(m)
    p = {**p2, **p}
    savedat(p, m, string)
    return True


################# Update the temperature profiles for Cluster 5-10 ##################
def create_temperature_profiles_high(
    outside_temperatures,
    max_flow_temp,
    min_flow_temp,
    max_return_temp,
    min_return_temp,
    flow_temp_threshold_high,
    flow_temp_threshold_low,
    return_temp_threshold_high,
    return_temp_threshold_low,
):
    """
    Create hourly temperature profiles for flow and return temperatures based on outside temperatures.

    Note: the flow temperature decreases as the outside temperature increases
    and the return temperature increases as the outside temperature increases.

    Parameters:
    - outside_temperatures: list or numpy array of hourly outside temperatures.
    - max_flow_temp: Maximum flow temperature.
    - min_flow_temp: Minimum flow temperature.
    - max_return_temp: Maximum return temperature.
    - min_return_temp: Minimum return temperature.
    - flow_temp_threshold_high: Outside temperature above which the flow temperature is minimum.
    - flow_temp_threshold_low: Outside temperature below which the flow temperature is maximum.
    - return_temp_threshold_high: Outside temperature below which the return temperature is maximum.
    - return_temp_threshold_low: Outside temperature above which the return temperature is minimum.

    Returns:
    - DataFrame with columns 'outside_temp', 'flow_temp', 'return_temp'
    """

    # Initialize lists to hold the temperatures
    flow_temps = []
    return_temps = []

    for outside_temp in outside_temperatures:
        # Calculate flow temperature
        if outside_temp > flow_temp_threshold_low:
            flow_temp = min_flow_temp
        elif outside_temp < flow_temp_threshold_high:
            flow_temp = max_flow_temp
        else:
            flow_temp = max_flow_temp - (max_flow_temp - min_flow_temp) * (
                outside_temp - flow_temp_threshold_high
            ) / (flow_temp_threshold_low - flow_temp_threshold_high)

        # Calculate return temperature
        if outside_temp > return_temp_threshold_low:
            return_temp = min_return_temp
        elif outside_temp < return_temp_threshold_high:
            return_temp = max_return_temp
        else:
            return_temp = min_return_temp + (max_return_temp - min_return_temp) * (
                outside_temp - return_temp_threshold_low
            ) / (return_temp_threshold_high - return_temp_threshold_low)

        # Append to lists
        flow_temps.append(flow_temp)
        return_temps.append(return_temp)

    # Create DataFrame
    df = pd.DataFrame(
        {
            "outside_temp": outside_temperatures,
            "flow_temp": flow_temps,
            "return_temp": return_temps,
        }
    )

    return df


def create_temperature_profiles_low(
    outside_temperatures,
    max_flow_temp,
    min_flow_temp,
    max_return_temp,
    min_return_temp,
    flow_temp_threshold_high,
    flow_temp_threshold_low,
    return_temp_threshold_high,
    return_temp_threshold_low,
):
    """
    Create hourly temperature profiles for flow and return temperatures based on outside temperatures.

    Note: the flow temperature decreases as the outside temperature increases
    and the return temperature increases as the outside temperature increases.

    Parameters:
    - outside_temperatures: list or numpy array of hourly outside temperatures.
    - max_flow_temp: Maximum flow temperature.
    - min_flow_temp: Minimum flow temperature.
    - max_return_temp: Maximum return temperature.
    - min_return_temp: Minimum return temperature.
    - flow_temp_threshold_high: Outside temperature above which the flow temperature is minimum.
    - flow_temp_threshold_low: Outside temperature below which the flow temperature is maximum.
    - return_temp_threshold_high: Outside temperature below which the return temperature is maximum.
    - return_temp_threshold_low: Outside temperature above which the return temperature is minimum.

    Returns:
    - DataFrame with columns 'outside_temp', 'flow_temp', 'return_temp'
    """

    # Initialize lists to hold the temperatures
    flow_temps = []
    return_temps = []

    for outside_temp in outside_temperatures:
        # Calculate flow temperature
        if outside_temp > flow_temp_threshold_low:
            flow_temp = min_flow_temp
        elif outside_temp < flow_temp_threshold_high:
            flow_temp = max_flow_temp
        else:
            flow_temp = max_flow_temp - (max_flow_temp - min_flow_temp) * (
                outside_temp - flow_temp_threshold_high
            ) / (flow_temp_threshold_low - flow_temp_threshold_high)

        # Calculate return temperature
        if outside_temp < return_temp_threshold_low:
            return_temp = max_return_temp
        elif outside_temp > return_temp_threshold_high:
            return_temp = min_return_temp
        else:
            return_temp = max_return_temp - (max_return_temp - min_return_temp) * (
                outside_temp - return_temp_threshold_low
            ) / (return_temp_threshold_high - return_temp_threshold_low)

            # return_temp = min_return_temp + (max_return_temp - min_return_temp) * (
            #     outside_temp - return_temp_threshold_low
            # ) / (return_temp_threshold_high - return_temp_threshold_low)

        # Append to lists
        flow_temps.append(flow_temp)
        return_temps.append(return_temp)

    # Create DataFrame
    df = pd.DataFrame(
        {
            "outside_temp": outside_temperatures,
            "flow_temp": flow_temps,
            "return_temp": return_temps,
        }
    )

    return df


def main(arg, *args, **kwargs):
    print(arg)
    return None


# =============================================================================
# main function
# =============================================================================
if __name__ == "__main__":

    file_path = BASE_DIR.joinpath("Profiles")
    file_name = "Scenarios Overview.xlsx"
    file_path = Path(file_path)

    data = pd.read_excel(file_path.joinpath(file_name), sheet_name="Profiles")

    el_price_high = pd.read_excel(
        file_path.joinpath(file_name), sheet_name="Electricity price - high"
    )
    el_price_low = pd.read_excel(
        file_path.joinpath(file_name), sheet_name="Electricity price - low"
    )

    # interpolate temperatures for data center and supermarket
    # datacenter_temp = interpolate_temperature(
    #     data["Data center (profile as capacity factor)"].values,
    #     32,
    #     39,
    #     44,
    #     49,
    #     0.1,
    #     0.5,
    #     0.7,
    #     1,
    # )
    # supermarket_temp = interpolate_temperature(
    #     data["Supermarket and metro station (capacity factor)"].values,
    #     12,
    #     15,
    #     18,
    #     22,
    #     0.1,
    #     0.5,
    #     0.7,
    #     1,
    # )

    # add_profile(
    #     short="datacenter",
    #     name="Data Center",
    #     year=0,
    #     profile=np.array(datacenter_temp),
    #     string="datacenter_temperature",
    #     new=True,
    # )

    # add_profile(
    #     short="supermarket",
    #     name="Supermarket and Metro",
    #     year=0,
    #     profile=np.array(supermarket_temp),
    #     string="river_temperature",
    #     new=False,
    # )

    # # add the electricity price profiles to the dat files
    # for y in range(2023, 2051):
    #     add_profile(
    #         short="low",
    #         name="Stockholm Low",
    #         year=y,
    #         profile=el_price_low[str(y)].values,
    #         string="price",
    #         new=False,
    #     )

    #     add_profile(
    #         short="high",
    #         name="Stockholm High",
    #         year=y,
    #         profile=el_price_high[str(y)].values,
    #         string="price",
    #         new=False,
    #     )

    # # add the outside temperature profiles to the dat files
    # add_profile(
    #     short="Sthlm",
    #     name="Stockholm",
    #     year=0,
    #     profile=data["Outdoor temp"].values,
    #     string="temperature",
    #     new=False,
    # )

    # # add the sewage water temperature profiles to the dat files
    # add_profile(
    #     short="Sthlm",
    #     name="Stockholm",
    #     year=0,
    #     profile=data["Sewage treatment plant source tempreatures"].values,
    #     string="wastewater_temperature",
    #     new=False,
    # )

    # # add the load profiles to the dat files
    # add_profile(
    #     short="Sthlm",
    #     name="Stockholm",
    #     year=0,
    #     profile=data["Load profile"].values,
    #     string="load",
    #     new=False,
    # )

    ################################################################################################
    ##### Generate grid temperature profiles #####
    ### High temperature ###
    outside_temperatures = data["Outdoor temp"].values
    high_max_flow_temp = 100
    high_min_flow_temp = 65
    high_max_return_temp = 50
    high_min_return_temp = 56
    high_flow_temp_threshold_high = -16
    high_flow_temp_threshold_low = 0
    high_return_temp_threshold_high = -18
    high_return_temp_threshold_low = 20

    # create high grid temperature profiles
    temperature_profiles_high = create_temperature_profiles_high(
        outside_temperatures=outside_temperatures,
        max_flow_temp=high_max_flow_temp,
        min_flow_temp=high_min_flow_temp,
        max_return_temp=high_max_return_temp,
        min_return_temp=high_min_return_temp,
        flow_temp_threshold_high=high_flow_temp_threshold_high,
        flow_temp_threshold_low=high_flow_temp_threshold_low,
        return_temp_threshold_high=high_return_temp_threshold_high,
        return_temp_threshold_low=high_return_temp_threshold_low,
    )

    temperature_profiles_high["deltat"] = (
        temperature_profiles_high.flow_temp - temperature_profiles_high.return_temp
    )

    # add the  high grid temperature profiles to the dat files
    add_profile(
        short="high",
        name="Stockholm High",
        year=0,
        profile=temperature_profiles_high["flow_temp"].values,
        string="inlet_temperature",
        new=True,
    )

    add_profile(
        short="high",
        name="Stockholm High",
        year=0,
        profile=temperature_profiles_high["return_temp"].values,
        string="return_temperature",
        new=True,
    )

    ### Medium temperature ###
    outside_temperatures = data["Outdoor temp"].values
    medium_max_flow_temp = 80
    medium_min_flow_temp = 55
    medium_max_return_temp = 50
    medium_min_return_temp = 40
    medium_flow_temp_threshold_high = -16
    medium_flow_temp_threshold_low = -6
    medium_return_temp_threshold_high = 20
    medium_return_temp_threshold_low = -16

    # create medium grid temperature profiles
    temperature_profiles_medium = create_temperature_profiles_low(
        outside_temperatures=outside_temperatures,
        max_flow_temp=medium_max_flow_temp,
        min_flow_temp=medium_min_flow_temp,
        max_return_temp=medium_max_return_temp,
        min_return_temp=medium_min_return_temp,
        flow_temp_threshold_high=medium_flow_temp_threshold_high,
        flow_temp_threshold_low=medium_flow_temp_threshold_low,
        return_temp_threshold_high=medium_return_temp_threshold_high,
        return_temp_threshold_low=medium_return_temp_threshold_low,
    )

    temperature_profiles_medium["deltat"] = (
        temperature_profiles_medium.flow_temp - temperature_profiles_medium.return_temp
    )

    for year in range(2023, 2051):
        if year < 2031:
            # add the high grid temperature profiles to the dat files until 2030
            add_profile(
                short="low",
                name="Stockholm Low",
                year=year,
                profile=temperature_profiles_high["flow_temp"].values,
                string="inlet_temperature",
                new=False,
            )

            add_profile(
                short="low",
                name="Stockholm Low",
                year=year,
                profile=temperature_profiles_high["return_temp"].values,
                string="return_temperature",
                new=False,
            )
        else:
            add_profile(
                short="low",
                name="Stockholm Low",
                year=year,
                profile=temperature_profiles_medium["flow_temp"].values,
                string="inlet_temperature",
                new=False,
            )

            add_profile(
                short="low",
                name="Stockholm Low",
                year=year,
                profile=temperature_profiles_medium["return_temp"].values,
                string="return_temperature",
                new=False,
            )

    ################################################################################################

    # add seawater temperature profiles
    seawater = pd.read_excel(file_path.joinpath("hourlyseawater.xlsx"))
    add_profile(
        short="Sthlm",
        name="Stockholm Seawater",
        year=0,
        profile=seawater.temperatures.values,
        string="seawater_temperature",
        new=True,
    )


# =============================================================================
####### Check profiles ########
price, price_mapper = opendat("price")
temperature, temperature_mapper = opendat("temperature")
wastewater_temperature, wastewater_temperature_mapper = opendat(
    "wastewater_temperature"
)
load, load_mapper = opendat("load")
radiation, radiation_mapper = opendat("radiation")
river_temperature, river_temperature_mapper = opendat("river_temperature")
datacenter_temp, datacenter_temp_mapper = opendat("datacenter_temperature")
inlet_temperature, inlet_temperature_mapper = opendat("inlet_temperature")
return_temperature, return_temperature_mapper = opendat("return_temperature")
seawater_temperature, seawater_temperature_mapper = opendat("seawater_temperature")
