import rasterio
import os as os 
from rasterio.warp import calculate_default_transform, reproject, Resampling
import rasterio.mask
import rasterio.merge
import numpy as np 
from skimage import morphology
import fiona 
import cv2
from rasterio.enums import MergeAlg
from numpy import int16
from rasterio import features
import geopandas as gpd
import pandas as pd

def writter(raster_target, array, output) :

    profile = rasterio.open(raster_target).profile
    
    profile["driver"] = "GTiff"
    profile["dtype"] = rasterio.uint8
    profile["compress"] = "lzw"
    
    with rasterio.open(output, "w", **profile) as dst :
        dst.write(array, 1) 



def get_target_shape(raster) : 
    return rasterio.open(raster).read(1).shape



def merge(raster_list, output_file) : 

    rasters_to_mosaic = []

    for raster in raster_list:
        src = rasterio.open(raster)
        rasters_to_mosaic.append(src)

    raster_merged, output_transform= rasterio.merge.merge(rasters_to_mosaic, method='max', nodata = np.nan)


    out_meta = rasters_to_mosaic[0].meta.copy()

            
    out_meta.update({"driver": "GTiff",
                            "compress": "lzw",
                            "height": raster_merged.shape[1],
                            "width": raster_merged.shape[2],
                            "transform": output_transform,
                            "nodata": 255})
            
    with rasterio.open(output_file, "w", **out_meta) as dst: 
            dst.write(raster_merged[0], 1)




def clean(change, date, output_change_path, output_date_path, min_size, min_occur, mask_f_nf, gabon_tiles, tampon, tile_name, code) : 

    print(" INFO || The resulted raster is being cleaned...")

    change_raster = rasterio.open(change)
    change_array  = change_raster.read(1)

    dates_array  = rasterio.open(date).read(1)
    
    print(" - Removing detections with an occurence lower than " + str(min_occur) + "...")
    msk = np.where(change_array > min_occur, 1, 0)

    print(" - Removing detections of less than " + str(min_size) + " pixels...")
    imglab  = morphology.label(msk) 
    cleaned = morphology.remove_small_objects(imglab, min_size=min_size, connectivity=2)

    output_changes = np.where(cleaned > 0, change_array, 0)
    output_dates   = np.where(cleaned > 0, dates_array , 0)
    
    shp_to_clip = gpd.read_file(mask_f_nf)
    mask = gpd.read_file(gabon_tiles)
    
    
    selected_tile = mask[mask['Name'] == tile_name]
  
    clipped_shp = gpd.overlay(shp_to_clip, selected_tile, how='intersection')
    clipped_shp = clipped_shp.to_crs(change_raster.crs)
  
    clipped_shp[code] = pd.to_numeric(clipped_shp[code])
    
    print(" - Applying the forest mask...")
    geom_value = [(geom,value) for geom, value in zip(clipped_shp.geometry, clipped_shp[code])]
    
    rasterized = features.rasterize(geom_value,
                                    out_shape = change_raster.shape,
                                    transform = change_raster.transform,
                                    all_touched = True,
                                    fill = -5,   # background value
                                    merge_alg = MergeAlg.replace,
                                    dtype = int16)
                                    
    mask_array = np.where(rasterized == 20, 1, 0)
    
    binary_changes = np.where(output_changes>0, 1,0)
    
    kernel = np.ones((tampon, tampon), np.uint8)
    
    img_dilation = cv2.dilate(mask_array.astype(np.uint8), kernel, iterations=1)
    
    output_changes = np.where(img_dilation > 0, 0, output_changes)
    output_dates   = np.where(img_dilation > 0, 0, output_dates)
 

    with rasterio.open(output_change_path, "w", **change_raster.profile) as dst :
        dst.write(output_changes, 1)
    with rasterio.open(output_date_path, "w",   **change_raster.profile) as dst :
        dst.write(output_dates, 1) 
    

    return output_change_path, output_date_path




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

    

