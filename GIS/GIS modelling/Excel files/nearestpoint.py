import pandas as pd
def dist_between_two_lat_lon(*args):
    from math import asin, cos, radians, sin, sqrt
    lat1, lat2, long1, long2 = map(radians, args)

    dist_lats = abs(lat2 - lat1) 
    dist_longs = abs(long2 - long1) 
    a = sin(dist_lats/2)**2 + cos(lat1) * cos(lat2) * sin(dist_longs/2)**2
    c = asin(sqrt(a)) * 2
    radius_earth = 6378 # the "Earth radius" R varies from 6356.752 km at the poles to 6378.137 km at the equator.
    return c * radius_earth

df_source =pd.read_excel('Sources.xlsx')
df_network = pd.read_excel('Network.xlsx')
# city = {'lat_key': value, 'lon_key': value}  # type:dict()
new_york = {'lat': 40.712776, 'lon': -74.005974}
washington = {'lat': 47.751076,  'lon': -120.740135}
san_francisco = {'lat': 37.774929, 'lon': -122.419418}

network_identifier_list = df_network['Identifier']
lat_list = df_network['lat']
long_list = df_network['long']

lat_to_find = df_source['Lat'] 
lon_to_find = df_source['Long']
source_name = df_source['Source']

sourcelat= []
sourcelon =[]
source_name_list = []
network_name = []
nearestlat = []
nearestlon = []
for i in range(0,len(lat_to_find)):
    distancelist = []
    for j in range(0,len(network_identifier_list)):
        distance = dist_between_two_lat_lon(lat_to_find[i], lat_list[j], lon_to_find[i], long_list[j])
        distancelist.append(distance)
    index = distancelist.index(min(distancelist))
    sourcelat.append(lat_to_find[i])
    sourcelon.append(lon_to_find[i])
    source_name_list.append(source_name[i])
    network_name.append(network_identifier_list[index])
    nearestlat.append(lat_list[index])
    nearestlon.append(long_list[index])
df = pd.DataFrame()
df['Source_name'] = source_name
df['sourcelat'] = sourcelat
df['sourcelon'] = sourcelon
df['network_name'] = network_name
df['nearestlat'] = nearestlat
df['nearestlon'] = nearestlon
df.to_excel('source_and_nearestpoints.xlsx')