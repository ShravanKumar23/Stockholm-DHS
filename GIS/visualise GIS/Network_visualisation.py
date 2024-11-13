import plotly.graph_objects as go

# Sample data: latitude and longitude coordinates
latitudes = [37.7749, 34.0522, 40.7128]  # Example latitudes
longitudes = [-122.4194, -118.2437, -74.0060]  # Example longitudes

# Plotting the points
fig = go.Figure(go.Scattermapbox(
    lat=latitudes,
    lon=longitudes,
    mode='markers',
    marker=dict(size=10, color='blue')
))

fig.update_layout(
    mapbox_style="open-street-map",
    mapbox=dict(
        center=dict(lat=38, lon=-94),
        zoom=3
    )
)

fig.show()
