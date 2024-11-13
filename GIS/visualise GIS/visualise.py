# importing the libraries
import plotly.express as px
import pandas as pd
px.set_mapbox_access_token('pk.eyJ1Ijoic2hyYXZhbmt1bWFyMjMiLCJhIjoiY2xtc3k4c2dnMDJpeTJrbGVma2hnYzQ5ZiJ9.vEjrtJ6FpOJlhjDgbQzomg')
countries = 'sources.xlsx'
data = pd.read_excel(countries)

# plotting map using plotly
# fig = px.scatter_geo(data, lat = 'Latitude', lon = 'Longitude', color = 'Name')

# # setting title for the map
# fig.update_layout(title = 'Countries', title_x = 0.5)

# fig.show()

fig = px.scatter_mapbox(data, lat='Latitude', lon = 'Longitude', color = 'Source type', size_max=10, zoom=8, title='Sources',mapbox_style='outdoors',size='Size', opacity=1 ,color_discrete_sequence=["darkmagenta", "green", "blue", "black", "red"])
# fig.update_traces(marker={"size": 20, "symbol": ["cross"]})
fig.show()