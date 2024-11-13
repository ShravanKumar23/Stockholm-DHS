import json
import pandas as pd
from module.functions.create_network import  run_create_network
from module.functions.optimize_network import run_optimize_network
from module.utilities.create_ex_grid import create_ex_grid
import warnings
import logging
import datetime as dt
import os
warnings.filterwarnings("ignore")

# sources_info = json.load(open("cres_sources_info_lt.json"))
# sinks_info = json.load(open("cres_sinks_info_lt.json"))

# n_supply_list = sources_info["n_supply_list"]
# n_demand_list = sinks_info["n_demand_list"]
# grid_specific = sinks_info["n_grid_specific"]
# n_thermal_storage = sinks_info["n_thermal_storage"]

idlist = []
losslist = []
costlist = []
namelistout =[]
lengthlist = []
totalcostlist = []
print(f"\t{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\t" f"Starting.")
inputDir = ".\Input_Data\\"
inputFile = 'Varta-Kungsholmen.xlsx'
inputPath = os.path.join(inputDir, inputFile)
df = pd.read_excel(io=inputPath, sheet_name='Connection points')
df1 = pd.read_excel(io=inputPath, sheet_name='Polygons - raw')
maindict = {}
poldict = {}
for k in range(1,2):
    intdf = df[df['Srno.'] == k]
    intdf.reset_index(inplace = True)
    del intdf['index']
    IDlist = intdf['ID'].to_list()
    caplist = intdf['Capacity'].to_list()
    suplatlist = intdf['Supply Lat'].to_list()
    suplonlist = intdf['Supply Lon'].to_list()
    namelist = intdf['Name'].to_list()
    intdict3 = {'ID' : IDlist, 'Capacity':caplist, 'supplylat': suplatlist, 'supplylon': suplonlist, 'Name': namelist} 
    intdict = { str(k) : intdict3}
    intdict2 = {'demandlat': intdf['Demand Lat'][0], 'demandlon': intdf['Demand Lon'][0]}
    intpoldict = {'P1' : [df1['P1 Lat'][k-1],df1['P1 Lon'][k-1]], 'P2' : [df1['P2 Lat'][k-1],df1['P2 Lon'][k-1]], 'P3' : [df1['P3 Lat'][k-1],df1['P3 Lon'][k-1]], 'P4' : [df1['P4 Lat'][k-1],df1['P4 Lon'][k-1]]} 
    intdict[str(k)].update(intdict2)
    intdict[str(k)].update(intpoldict)
    maindict.update(intdict)
for i in maindict:
    try:
        n_demand_list = [{'id': 999, 'coords': [maindict[i]['demandlat'], maindict[i]['demandlon']], 'cap': sum(maindict[i]['Capacity'])}]
        polygon = [(maindict[i]['P1'][0], maindict[i]['P1'][1]), (maindict[i]['P2'][0], maindict[i]['P2'][1]), (maindict[i]['P3'][0], maindict[i]['P3'][1]), (maindict[i]['P4'][0], maindict[i]['P4'][1])]
        n_supply_list = []
        for k in range(0,len(maindict[i]['ID'])):
            n_supply_list.append({'id': int(maindict[i]['ID'][k]), 'coords': [maindict[i]['supplylat'][k], maindict[i]['supplylon'][k]], 'cap': maindict[i]['Capacity'][k]})
        grid_specific = []
        n_thermal_storage = []

        network_resolution = "high"

        ex_grid_data_json = []
        ex_cap_json = []

        # NOTE: change ex_cap path

        input_data = {
            "platform": {
                "ex_grid": ex_grid_data_json,
                "network_resolution": network_resolution,
                "polygon": polygon,
            },
            "cf-module": {
                "n_supply_list": n_supply_list,
                "n_demand_list": n_demand_list,
                "n_grid_specific": grid_specific,
                "n_thermal_storage": n_thermal_storage,
            },
            "teo-module": {
                "ex_cap": ex_cap_json
            },
        }

        from module.utilities import kb, kb_data

        create_output = run_create_network(input_data, kb_data.kb)

        # Prepare for run_optimize_network(input_data)

        ###########INVESTMENT COSTS PUMPS###############

        invest_pumps = 0

        ###########COSTS DIGGING STREET#################
        fc_dig_st = 350
        vc_dig_st = 700

        ###########COSTS DIGGING TERRAIN#################
        fc_dig_tr = 200
        vc_dig_tr = 500

        ###########COSTS PIPES###########################
        fc_pip = 50
        vc_pip = 700

        ###########COST FORMULAS EXPONENTS###############
        vc_dig_st_ex = 1.1
        vc_dig_tr_ex = 1.1
        vc_pip_ex = 1.3

        #####COST DIFFERENCE FACTOR STREET/TERRAIN &
        # STREET/OVERLAND#######

        factor_street_terrain = 0.10
        factor_street_overland = 0.4

        ###########GRID TEMPERATURES/WATER FEATURES/HEAT CAP.####

        flow_temp = 100
        return_temp = 70
        water_den = 1000
        heat_capacity = 4.18

        ###########ENVIRONMENTAL INPUTS#################

        ground_temp = 5
        ambient_temp = 15

        ###########DATAFRAME FOR SURFACE LOSSES#################

        surface_losses_json = [
            {"dn": 0.02, "overland_losses": 0.115994719393908},
            {"dn": 0.025, "overland_losses": 0.138092834981244},
            {"dn": 0.032, "overland_losses": 0.15109757219986},
            {"dn": 0.04, "overland_losses": 0.171799705290563},
            {"dn": 0.05, "overland_losses": 0.193944276611768},
            {"dn": 0.065, "overland_losses": 0.219829984514374},
            {"dn": 0.08, "overland_losses": 0.231572190233268},
            {"dn": 0.1, "overland_losses": 0.241204678239951},
            {"dn": 0.125, "overland_losses": 0.280707496411117},
            {"dn": 0.15, "overland_losses": 0.320919871727017},
            {"dn": 0.2, "overland_losses": 0.338510752592325},
            {"dn": 0.25, "overland_losses": 0.326870584772369},
            {"dn": 0.3, "overland_losses": 0.376259860034531},
            {"dn": 0.35, "overland_losses": 0.359725182960969},
            {"dn": 0.4, "overland_losses": 0.372648018718974},
            {"dn": 0.45, "overland_losses": 0.427474040273953},
            {"dn": 0.5, "overland_losses": 0.359725658523504},
            {"dn": 0.6, "overland_losses": 0.420023799255459},
            {"dn": 0.7, "overland_losses": 0.478951907501331},
            {"dn": 0.8, "overland_losses": 0.540336445060049},
            {"dn": 0.9, "overland_losses": 0.600053256925217},
            {"dn": 1.0, "overland_losses": 0.662751592458654},
        ]

        input_optimization_data = {
            "gis-module": {
                "nodes": create_output["nodes"],
                "edges": create_output["edges"],
                "demand_list": create_output["demand_list"],
                "supply_list": create_output["supply_list"],
            },
            "platform": {
                "ex_grid": ex_grid_data_json,
                "network_resolution": network_resolution,
                "water_den": water_den,
                "factor_street_terrain": factor_street_terrain,
                "factor_street_overland": factor_street_overland,
                "heat_capacity": heat_capacity,
                "flow_temp": flow_temp,
                "return_temp": return_temp,
                "surface_losses_dict": surface_losses_json,
                "ground_temp": ground_temp,
                "ambient_temp": ambient_temp,
                "fc_dig_st": fc_dig_st,
                "vc_dig_st": vc_dig_st,
                "vc_dig_st_ex": vc_dig_st_ex,
                "fc_dig_tr": fc_dig_tr,
                "vc_dig_tr": vc_dig_tr,
                "vc_dig_tr_ex": vc_dig_tr_ex,
                "fc_pip": fc_pip,
                "vc_pip": vc_pip,
                "vc_pip_ex": vc_pip_ex,
                "invest_pumps": invest_pumps,
            },
            "cf-module": {"n_supply_list": n_supply_list, "n_demand_list": n_demand_list},
            "teo-module": {"ex_cap": ex_cap_json},
        }

        optimize_output = run_optimize_network(input_optimization_data, kb_data.kb)

        # Writing GIS' output
        # with open("GIS_CRESOutput_itr1.json", "w") as output:
        #     output.write(str(optimize_output))

        idlist.append(i)
        namelistout.append(maindict[i]['Name'][0])
        costlist.append(optimize_output['losses_cost_kw']['cost_in_kw'])
        losslist.append(optimize_output['losses_cost_kw']['losses_in_kw'])
        lengthlist.append(optimize_output['sums']['length'])
        totalcostlist.append(optimize_output['sums']['total_costs'])
    except:
        idlist.append(i)
        costlist.append(0)
        losslist.append(0)
        lengthlist.append(0)
        namelistout.append(0)
        totalcostlist.append(0)
    print(f"\t{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\t" f"{i} is done.")
    #print(str(i) + 'Done')
outdf = pd.DataFrame()
outdf['ID'] = idlist
outdf['Name'] = namelistout
outdf['Cost'] = costlist
outdf['Losses'] = losslist
outdf['Length'] = lengthlist
outdf['Total_Cost'] = totalcostlist

outdf.to_excel('Varta-Kungsholmen_output.xlsx')
# # Writing GIS' output
# with open('result_DHS_EH_cap.json', 'w') as fp:
#     json.dump(optimize_output, fp)

print(f"\t{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\t" f"Finished.")