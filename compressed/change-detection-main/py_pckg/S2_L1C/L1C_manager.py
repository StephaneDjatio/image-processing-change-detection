import os  
import re

from eodag import EODataAccessGateway

import json 
import pandas as pd

"""
DOWNLOADER.PY Module 
This python module allows to process, download L1C files using PEPS. 
"""

def search(tile_name, sta_date, end_date, json_available_L1C) :

    print(f'-> Checking for new acquisitions : asking for availables L1C (Tile : {tile_name}, Period : {sta_date} - {end_date})')

    # Changing date format
    sta = str(sta_date)[0:4] + "-" + str(sta_date)[4:6] + "-" + str(sta_date)[6:8]
    end = str(end_date)[0:4] + "-" + str(end_date)[4:6] + "-" + str(end_date)[6:8]

    # Opening our gateway and setting PEPS as our provider
    dag = EODataAccessGateway()
    #dag.set_preferred_provider("peps")
    
    # Setting our search criterias
    search_criteria = {
    "productType": "S2_MSI_L1C",
    "start": sta,
    "end": end,
    "tileid":tile_name
    }

    try : 
        # Launching the search
        products_found = dag.search_all(**search_criteria)

        # Saving the result as a json file
        dag.serialize(products_found, filename=json_available_L1C)
        
        # Opening the json file
        file_available_L1C = open(json_available_L1C)
        L1C_json_content   = json.load(file_available_L1C)

        # Initiate the list containing the names of the available L1C files...
        L1C_available = []

        #... and adding the L1C file names to this list
        for vars in L1C_json_content['features'] : 
            L1C_available.append(vars['id'])
    except : 

        print("\n /!\ Be carefull ! The L1C providers service seem to be down. The new L1C acquisitions will not be downloaded /!\ \n") 
        L1C_available = []

    return L1C_available




def check(tile_name, sta_date, end_date, L1C_path) :

    # Initiate dataframe containing L1C files
    df_L1C = pd.DataFrame(columns=['Date', 'Files'])
    L1C_dates     = []
    L1C_files     = []
    L1C_paths     = []

    # Getting the path to the L1C files (for this specific tile)
    S2_L1C_tile_path = os.path.join(L1C_path, tile_name)
    
    # Initiate a list of all the L1C already available ...
    local_L1C = []
    
    # ... Fill it 
    for files in os.listdir(S2_L1C_tile_path):
        if "MSIL1C" in files and ".SAFE" in files and not ".zip" in files : 
            L1C_files.append(re.sub('.SAFE', '', files, 1))
            L1C_paths.append(os.path.join(L1C_path, re.sub('.SAFE', '', files, 1)))
            L1C_dates.append(int(files.split('_MSIL1C_')[1].split('T')[0]))

    df_L1C['Date']    = L1C_dates
    df_L1C['Files']   = L1C_files
    df_L1C['Paths']  = L1C_paths

    df_L1C = df_L1C.loc[~(df_L1C['Date'] < int(sta_date))]
    df_L1C = df_L1C.loc[~(df_L1C['Date'] > int(end_date))]

    local_L1C = df_L1C['Paths'].tolist()

    # ... Return it
    return local_L1C



def get_missing(L2A_local, L2A_unconst, L1C_available) :
    
    missing_L1C = []

    # Get the S2 descriptors
    L2A_descriptors = [x.split("_MSIL2A_")[1].split("_N")[0] for x in L2A_local]
    L1C_descriptors = [x.split("_MSIL1C_")[1].split("_N")[0] for x in L1C_available]

    # Finding the L1C that are available on PEPS and not already downloaded
    for L1C_desc, L1C_files in zip(L1C_descriptors, L1C_available) :
        if L1C_desc not in L2A_descriptors : 
            missing_L1C.append(L1C_files)

    # Get the S2 descriptors
    L2A_descriptors = [x.split("_MSIL2A_")[1].split("_N")[0] for x in L2A_unconst]

    # Finding the L1C that are available on PEPS and not already downloaded
    for L1C_desc, L1C_files in zip(L1C_descriptors, L1C_available) :
        if L1C_desc in L2A_descriptors : 
            missing_L1C.append(L1C_files)

    
    return missing_L1C




def L1C_purge(L2A_local, L1C_local):
     
    L1C_to_remove = []

    # Get the S2 descriptors
    L2A_descriptors = [x.split("_MSIL2A_")[1].split("_N")[0] for x in L2A_local]
    L1C_descriptors = [x.split("_MSIL1C_")[1].split("_N")[0] for x in L1C_local]

    # Check for L1C files already processed as L2A
    for L1C_desc, L1C_files in zip(L1C_descriptors, L1C_local) :
        if L1C_desc in L2A_descriptors : 
            L1C_to_remove.append(L1C_files)
    
    # Purge 
    print("Purge : ")
    for L1C in L1C_to_remove :
        print("Removing " + L1C)

    




def L1C_download(L1C_files_to_download, L1C_available_json, L1C_dir, tile_name):

    os.environ["EODAG__PEPS__DOWNLOAD__OUTPUTS_PREFIX"] = os.path.join(L1C_dir, tile_name)

    dag = EODataAccessGateway()
    dag.set_preferred_provider("peps")

    # We open it in order to get the EOProduct format (necessary to download the product.)
    deserialized_search_results = dag.deserialize(L1C_available_json)

    # This function allows to download the new L1C datas

    plugin_conf = {"extract": True}

    for products in  deserialized_search_results : 
        if products.as_dict()['id'] in L1C_files_to_download : 
            print(f'Downloadind L1C product : ' + products.as_dict()['id'])
            try : 
                dag.download(products, **plugin_conf)
            except :
                print("/!\ EODAG did not allow you to download this product : " + products)
