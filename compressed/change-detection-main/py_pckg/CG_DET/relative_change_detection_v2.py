from py_pckg.CG_DET import index_calc
import matplotlib.pyplot as plt 
import numpy as np


def process(S2_L2A_obj, df_tile, threshold1, threshold2) : 

    
    
    for idx, S2_L2A in enumerate(S2_L2A_obj) : 

        print("-> Calculating NBR for and extracting changes from :")
        print(S2_L2A)

        nbr, mask = index_calc.get_nbr(S2_L2A_obj[S2_L2A])
 
        if idx == 0 : 
            NBR_changes = (nbr*0).astype(int)
            NBR_filled  = np.where(mask==False, nbr,-9999)
            NBR_high    = (nbr*0).astype(int)
            tmp =  NBR_changes
        else : 
            NBR_overlap = np.where((NBR_filled > -1) & (mask == False), 1, 0)
            NBR_diff    = np.where(NBR_overlap==1, NBR_filled - nbr, 0)
            NBR_dete    = np.where((NBR_filled>0.57) & (NBR_diff > threshold1), 1, 0)
            NBR_high    = np.where((NBR_dete==1) & (mask==False), NBR_filled, NBR_high)
            NBR_changes = np.where(NBR_dete==1, 1, np.where((tmp>=1) & (abs(nbr-NBR_high)<threshold2) & (mask==False) & (NBR_dete != 1),0, np.where((tmp>0) & (mask==False)&(abs(nbr-NBR_high)>threshold2) ,NBR_changes+1, NBR_changes)))
            tmp = NBR_changes
            NBR_filled  = np.where(mask==False, nbr, np.where(NBR_filled>-1, NBR_filled, -9999))
    
    
            

    
 
    dates="bla"
        
    return dates, NBR_changes


