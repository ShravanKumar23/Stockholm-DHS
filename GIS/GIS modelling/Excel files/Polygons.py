import geopandas
from shapely.geometry import Polygon, LineString, Point, MultiPoint
s = geopandas.GeoSeries(
    [
        Point(59.2787601, 18.0022865),
        Point(59.283542766, 18.029651628),
    ]
)
print(s.convex_hull)