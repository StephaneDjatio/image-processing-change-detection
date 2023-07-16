import geopandas as gpd
import pandas as pd 
import fiona
from shapely.geometry import Polygon
import os as os 
import json




def L8_corresponding_to_S2(tile_name, gabon_shapes_S2, gabon_shapes_LS) :

    corresponding_LS = []

    with fiona.open(gabon_shapes_S2, "r") as shapefile:
            for f in shapefile:
                if f['properties']['Name'] == tile_name : 
                    p1 = f['geometry']['coordinates']
                    p1 =Polygon(f['geometry']['coordinates'][0])

                    with fiona.open(gabon_shapes_LS, "r") as shapefile:
                        for f in shapefile:
                            p2 = f['geometry']['coordinates']
                            p2 = Polygon(f['geometry']['coordinates'][0])

                            if(p1.intersects(p2)) :
                                corresponding_LS.append(f['properties']['WRSPR'])

    return corresponding_LS

