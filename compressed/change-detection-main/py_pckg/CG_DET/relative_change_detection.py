from py_pckg.CG_DET import index_calc
import matplotlib.pyplot as plt 
import numpy as np


def process(S2_L2A_obj, df_tile, threshold1, threshold2) : 
    
    for idx, S2_L2A in enumerate(S2_L2A_obj) : 

        print("-> Calculating NBR for and extracting changes from :")
        print(S2_L2A)

        nbr, mask = index_calc.get_nbr(S2_L2A_obj[S2_L2A])

        if idx == 0 :
            NBR_filled  = np.where(mask==False, nbr, -9999) 
            NBR_changes = (nbr * 0).astype(int)
            NBR_high = (nbr * 0).astype(int)
            #NBR_low = (nbr * 0).astype(int)
        if idx > 0 : 
            # Get the areas where the analyse can be made 
            NBR_overlap = np.where((NBR_filled > -1) & (mask == False), 1, 0)


            NBR_diff    = np.where(NBR_overlap==1, NBR_filled - nbr, 0)
        
            NBR_dete    = np.where(NBR_diff > threshold1, 1, 0)
            #NBR_corr    = np.where(NBR_diff < threshold2, 1, 0)

            NBR_high = np.where((NBR_dete==1)&(mask==False), NBR_filled, NBR_high)
            #NBR_low  = np.where((NBR_dete==1)&(mask==False), nbr,      NBR_low)

            if idx == 1 : 
                NBR_changes = NBR_dete
            else : 
                NBR_changes = np.where(NBR_dete==1, 1, NBR_changes)
                NBR_changes = np.where((NBR_changes>0) & (abs(nbr-NBR_high)<0.15) & (mask==False) & (NBR_dete != 1),0, np.where((NBR_changes>0) & (mask==False) ,NBR_changes+1, NBR_changes))
                """
                NBR_changes = np.where((mask==False) & (NBR_corr!=1) & (NBR_changes>=1), NBR_changes + 1, NBR_changes)
                NBR_changes = np.where((NBR_corr==1) & (NBR_changes>0), 0, NBR_changes)
                NBR_changes = np.where(NBR_dete ==1, 1, NBR_changes)
                """
                
            NBR_filled  = np.where(mask==False, nbr, np.where(NBR_filled>-1, NBR_filled, -9999)) 
    
    
            

    
 
    dates="bla"
        
    return dates, NBR_changes


