import pandas as pd
import geopandas as gpd
import rasterio 
import fiona
import rasterio.features

"""
This module handles the vector management
"""

def raster_to_vector(raster_path, vector_path, attribute_name) :

    schema = {"geometry": "Polygon", "properties": {attribute_name: "int"}}

    with rasterio.open(raster_path) as raster:
        image = raster.read()
        # use your function to generate mask
        # and convert to uint8 for rasterio.features.shapes
        mask = image.astype('uint8')
        shapes = rasterio.features.shapes(mask, transform=raster.transform)
        # select the records from shapes where the value is > 0        
        records = [{"geometry": geometry, "properties": {attribute_name: value}}
                for (geometry, value) in shapes if value > 0]
        with fiona.open(vector_path, "w", "ESRI Shapefile", crs=raster.crs.data, schema=schema) as out_file:
            out_file.writerecords(records)


def merge_vectors(vector1, att1, vector2, att2, output_vector) :

    # Charger les deux vecteurs en tant que dataframes
    df1 = gpd.read_file(vector1)
    df2 = gpd.read_file(vector2)

    # Fusionner les deux dataframes en utilisant la colonne "geometry" comme clé de fusion
    merged_df = df1.merge(df2, on='geometry')

    # Sélectionner les colonnes "date" et "change"
    vec1_column = merged_df[att1]
    vec2_column = merged_df[att2]

    # Créer un nouveau dataframe avec les colonnes "date" et "change"
    new_df = gpd.GeoDataFrame({
        att1: vec1_column,
        att2: vec2_column,
        'geometry': merged_df['geometry']
    })

    # Sauvegarder le nouveau dataframe en tant que vecteur dans un format de fichier géospatial
    new_df.to_file(output_vector)


def fill_vector(merged_vec, csv, output_vec):

    csv_file = pd.read_csv(csv, sep=",")
    csv_dict = csv_file.to_dict()
    

    dframe   = gpd.read_file(merged_vec)

    dframe['dates']  = dframe['date'].map(csv_dict['acq_dates'])
    dframe['sensor'] = dframe['date'].map(csv_dict['sensor'])
    dframe['file']   = dframe['date'].map(csv_dict['acq_pathes'])
    dframe['tile']   = dframe['date'].map(csv_dict['tile'])

   
    dframe.to_file(output_vec)



def combine_shapefiles(shapefile_paths, output_path):
    # Charger les shapefiles en tant que GeoDataFrames
    gdfs = []
    for path in shapefile_paths:
        gdf = gpd.read_file(path)
        gdfs.append(gdf)
    
    # Fusionner les GeoDataFrames en un seul GeoDataFrame
    merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)
    print(merged)
    
    # Dissoudre les polygones qui se chevauchent en un seul polygone
    merged = gpd.overlay(merged, merged, how='union', make_valid=True)

    merged.drop('date_2',       axis=1, inplace=True)
    merged.drop('dates_2',      axis=1, inplace=True)
    merged.drop('sensor_2',     axis=1, inplace=True)
    merged.drop('file_2',       axis=1, inplace=True)
    merged.drop('tile_2',       axis=1, inplace=True)
    merged.drop('occurence_2',   axis=1, inplace=True)

    merged = merged.rename(columns={"date_1": "index", "dates_1": "dates", "sensor_1": "sensor", "file_1": "file", "tile_1": "tile", "occurence_1": "occurence" })
    
    merged['dates'] = pd.to_datetime(merged['dates'], format='%Y%m%d')

    merged['dates'] = merged['dates'].apply(lambda x: x.strftime('%Y-%m-%d'))

    # Écrire le GeoDataFrame résultant dans un fichier shapefile
    merged.to_file(output_path)