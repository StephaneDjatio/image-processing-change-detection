from py_pckg.CG_DET import index_calc
import matplotlib.pyplot as plt 
import numpy as np
import pandas as pd 
import rasterio
from skimage import morphology


def get_target_crs(S2_L2A_obj):

    rast_ex_path = S2_L2A_obj.arbo_dict[next(iter(S2_L2A_obj.arbo_dict))]["RED"]
    rast_ex     = rasterio.open(rast_ex_path)
    rast_ex_crs = rast_ex.crs

    return rast_ex_crs



def cleaner(array_in,  pixel_size) : 
  
    binary  = np.where(array_in > 0, 1, 0 )
    imglab  = morphology.label(binary) # create labels in segmented image
    cleaned = morphology.remove_small_objects(imglab, min_size=pixel_size, connectivity=2)
    
    mask   = np.where(cleaned == 0, 1, 0 )
    raster_cleaned_size = np.where(mask==1, binary, 0)
    

    return raster_cleaned_size
    

def process(S2_L2A_obj, L8_obj_list, target_shape, S2_shapes, tmp_path, tile,  S2_th_bottom ,S2_th_top , LS_th_bottom, LS_th_top, raster_targ, max_size) : 

    acq_datf_S2_L8   = S2_L2A_obj.acqDataframe
    acq_dict_S2_L8   = S2_L2A_obj.arbo_dict
    
    for L8_obj in L8_obj_list : 
        acq_datf_S2_L8 = pd.concat([acq_datf_S2_L8,L8_obj.acqDataframe])
        acq_dict_S2_L8.update(L8_obj.arbo_dict)
    
    acq_datf_S2_L8 = acq_datf_S2_L8.sort_values('acq_dates')

    target_crs = get_target_crs(S2_L2A_obj)
    
    inc = 0
    
    print("\n INFO ||  Entering the change detection process.  \n")
    print("\n INFO || " + str(len(acq_datf_S2_L8)) + " acquisitions found. Starting the NBR thresholding. ")
    
    for idx, row in acq_datf_S2_L8.iterrows():
        
        print("\n")
        print(" *** " + str(inc+1) + "/" + str(len(acq_datf_S2_L8)) + " *** ")
        print(row)
        print("\n")

        if row["sensor"] == "S2" : 
            th_top    = float(S2_th_top)
            th_bottom = float(S2_th_bottom)
        else : 
            th_top    = float(LS_th_top)
            th_bottom = float(LS_th_bottom)
        
        nbr, mask = index_calc.get_nbr(acq_dict_S2_L8[row['acq_pathes']], tmp_path, target_crs, S2_shapes, tile, target_shape, rasterio.open(raster_targ).profile)
        arr_to_analyse   = np.where((nbr > th_top) & (nbr < 1) & (mask == False), 1, 0)
        mask             = mask | np.where((nbr <= th_top) & (nbr >= th_bottom), True, False)

        if inc == 0 : 
            potential = arr_to_analyse
            change    = arr_to_analyse * 0
            dates     = arr_to_analyse * 0
        else : 
            changes_live = np.where((arr_to_analyse == 0) & (potential == 1) & (mask == False), 1, 0) 
            changes_live = cleaner(changes_live, max_size)
            if inc == 1 : 
                change = changes_live 
            else : 
                change_add   = np.where((change > 0) & (arr_to_analyse == 0) & (mask == False) & (changes_live != 1), 1, 0)
                tmp = change
                change       = np.where(mask == True, change, np.where(changes_live==1,1, np.where((tmp > 0) & (change_add==1), tmp + 1, 0)))


            potential = np.where((arr_to_analyse == 1) & (mask == False) | (potential == 1) & (mask == True), 1 ,0 )
            dates     = np.where((dates > 0) & (change > 0), dates, np.where(changes_live==1, inc, 0))
        
        inc += 1

    return dates, change, acq_datf_S2_L8



