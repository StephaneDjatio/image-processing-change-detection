import rasterio 
import cv2
import numpy as np 
import os as os 
import matplotlib.pyplot as plt 
import geopandas as gpd
import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


def reproject(raster, raster_target, target_crs, gabon_shapes_S2, tmp_output, tile, dtype) :

    print("\n PROCESS || Cropping the L8/L9 acquisition (and its mask) to make it fit the S2 shape.  \n")

    target_prf = rasterio.open(raster_target).profile

    target_prf["driver"] = "GTiff"
    target_prf["dtype"] = dtype
    target_prf["compress"] = "lzw"

    output_tmp_raster = os.path.join(tmp_output, "tmp_" + tile + ".tif")
    output_tmp_raster_reproj = os.path.join(tmp_output, "tmp_reproj_" + tile + ".tif")

    with rasterio.open(output_tmp_raster, "w", **target_prf) as dst : 
        dst.write(raster, 1)
    
    world = gpd.read_file(gabon_shapes_S2)
    polygone_cropper = world[world['Name'] == tile]

    world2 = world.to_crs({'init': str(target_crs)})
    world_path = os.path.join(tmp_output, "tmp_world.shp")
    world2.to_file(world_path)

    
    cmd = "gdalwarp -cutline \""+ world_path +"\" -crop_to_cutline  -cwhere \"Name='"+ tile +"'\" " + output_tmp_raster + " " + output_tmp_raster_reproj +" -overwrite" 
    os.system(cmd)

    print("\n")

    raster_reprojected = rasterio.open(output_tmp_raster_reproj).read(1)

    return raster_reprojected


def get_nbr(acq, tmp_path, target_crs, S2_shapes , tile, target_shape, prf) :

    if "MSIL2A" in acq['RED'] : 
        date = int(acq['RED'].split("_MSIL2A_")[1].split("T")[0])

        if date < 20220125 : 
            offset = 0
        else : 
            offset = 1000
  
        B12 = rasterio.open(acq['SWIR2']).read(1).astype(float)
        B08 = rasterio.open(acq['NIR']).read(1).astype(float)
        MSK = rasterio.open(acq['MASK']).read(1)

        B12 = cv2.resize(B12, B08.shape, cv2.INTER_CUBIC)
        MSK = np.where((MSK>=7) | (MSK<=3), 100, 10)
        MSK = cv2.resize(MSK.astype(np.uint8), B08.shape, cv2.INTER_NEAREST)

        MSK = np.where(MSK==100, True, False)

        NBR = ((B08-offset) - (B12-offset)) / ((B08-offset) + (B12-offset))
        
     

        
        
    else :

        B12 = rasterio.open(acq['SWIR2']).read(1).astype(float)
        B08 = rasterio.open(acq['NIR']).read(1).astype(float)
        MSK = rasterio.open(acq['MASK']).read(1)

        MSK_2 = np.bitwise_and(MSK, 2**2 ) > 0
        MSK_1 = np.bitwise_and(MSK, 2**1 ) > 0
        MSK_3 = np.bitwise_and(MSK, 2**3 ) > 0
        MSK_4 = np.bitwise_and(MSK, 2**4 ) > 0
        MSK_0 = np.bitwise_and(MSK, 2**0 ) > 0

        MSK = np.logical_or(MSK_2, np.logical_or(MSK_3, np.logical_or(MSK_1, np.logical_or(MSK_0, MSK_4))))

        MSK = np.where(MSK > 0, 0, 1)

    
        #MSK = np.where(MSK_6>0, False, True)
        #MSK = np.logical_or(MSK_2, np.logical_or(MSK_3, MSK_6)) 

        MSK = reproject(MSK, acq['RED'], target_crs, S2_shapes, tmp_path, tile, np.uint16)
        MSK = cv2.resize(MSK, target_shape, cv2.INTER_NEAREST).astype(np.uint16)

        MSK = np.where(MSK==0, True, False)
        

        NBR = ((B08 - B12) / (B08 + B12))
        NBR = reproject(NBR, acq['RED'], target_crs, S2_shapes, tmp_path, tile, rasterio.float32)
        
        NBR = cv2.resize(NBR, target_shape, cv2.INTER_CUBIC)



    return NBR , MSK


