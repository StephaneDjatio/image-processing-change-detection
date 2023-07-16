import os, shutil
import re

from eodag import EODataAccessGateway

import json
import pandas as pd

from glob import glob
import time

"""
DOWNLOADER.PY Module 
This python module allows to process, download L1C files using PEPS. 
"""


def consistancy_checker(L1C, file, tile):
    L1C_path = os.path.join(L1C, tile, file)

    try:
        B1_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B02.jp2"))[0]
    except:
        B1_band = "no_data"

    try:
        B2_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B02.jp2"))[0]
    except:
        B2_band = "no_data"

    try:
        B3_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B03.jp2"))[0]
    except:
        B3_band = "no_data"

    try:
        B4_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B04.jp2"))[0]
    except:
        B4_band = "no_data"

    try:
        B5_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B05.jp2"))[0]
    except:
        B5_band = "no_data"

    try:
        B6_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B06.jp2"))[0]
    except:
        B6_band = "no_data"

    try:
        B7_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B07.jp2"))[0]
    except:
        B7_band = "no_data"

    try:
        B8_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B08.jp2"))[0]
    except:
        B8_band = "no_data"

    try:
        B8A_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B8A.jp2"))[0]
    except:
        B8A_band = "no_data"

    try:
        B9_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B09.jp2"))[0]
    except:
        B9_band = "no_data"

    try:
        B10_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B10.jp2"))[0]
    except:
        B10_band = "no_data"

    try:
        B11_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B11.jp2"))[0]
    except:
        B11_band = "no_data"

    try:
        B12_band = glob(os.path.join(L1C_path, "*.SAFE", "GRANULE", "*", "IMG_DATA", "*B12.jp2"))[0]
    except:
        B12_band = "no_data"

    bands = [B1_band, B2_band, B3_band, B4_band, B5_band, B6_band, B7_band, B8_band, B8A_band, B9_band, B10_band,
             B11_band, B12_band]

    if "no_data" in bands:
        return False
    else:
        return True


def search(tile_name, sta_date, end_date, json_available_L1C, peps_id, peps_pwd):
    os.environ["EODAG__PEPS__AUTH__CREDENTIALS__USERNAME"] = peps_id
    os.environ["EODAG__PEPS__AUTH__CREDENTIALS__PASSWORD"] = peps_pwd

    print(
        f'\n PROCESS || Checking for new acquisitions : asking for availables L1C (Tile : {tile_name}, Period : {sta_date} - {end_date}) \n')

    # Changing date format
    sta = str(sta_date)[0:4] + "-" + str(sta_date)[4:6] + "-" + str(sta_date)[6:8]
    end = str(end_date)[0:4] + "-" + str(end_date)[4:6] + "-" + str(end_date)[6:8]

    # Opening our gateway and setting PEPS as our provider
    dag = EODataAccessGateway()
    # dag.set_preferred_provider("peps")

    # Setting our search criterias
    search_criteria = {
        "productType": "S2_MSI_L1C",
        "start": sta,
        "end": end,
        "tileid": tile_name
    }

    # try :
    # Launching the search

    products_found = dag.search_all(**search_criteria)
    # Saving the result as a json file
    dag.serialize(products_found, filename=json_available_L1C)

    # Opening the json file
    file_available_L1C = open(json_available_L1C)
    L1C_json_content = json.load(file_available_L1C)

    # Initiate the list containing the names of the available L1C files...
    L1C_available = []

    # ... and adding the L1C file names to this list
    for vars in L1C_json_content['features']:
        L1C_available.append(vars['id'])
    """
    except : 
        print("\n /!\ Be carefull ! The L1C providers service seem to be down. The new L1C acquisitions will not be downloaded /!\ \n") 
        L1C_available = []
    """
    return L1C_available


def check(tile_name, sta_date, end_date, L1C_path):
    # Initiate dataframe containing L1C files
    df_L1C = pd.DataFrame(columns=['Date', 'Files'])
    L1C_dates = []
    L1C_files = []
    L1C_paths = []

    # Getting the path to the L1C files (for this specific tile)
    S2_L1C_tile_path = os.path.join(L1C_path, tile_name)

    # Initiate a list of all the L1C already available ...
    local_L1C = []

    # ... Fill it
    for files in os.listdir(S2_L1C_tile_path):
        if "MSIL1C" in files and ".SAFE" in files and not ".zip" in files:
            L1C_files.append(re.sub('.SAFE', '', files, 1))
            L1C_paths.append(os.path.join(L1C_path, re.sub('.SAFE', '', files, 1)))
            L1C_dates.append(int(files.split('_MSIL1C_')[1].split('T')[0]))

    df_L1C['Date'] = L1C_dates
    df_L1C['Files'] = L1C_files
    df_L1C['Paths'] = L1C_paths

    df_L1C = df_L1C.loc[~(df_L1C['Date'] < int(sta_date))]
    df_L1C = df_L1C.loc[~(df_L1C['Date'] > int(end_date))]

    local_L1C = df_L1C['Paths'].tolist()

    # ... Return it
    return local_L1C


def get_local(tile_name, sta_date, end_date, L1C_path):
    # Initiate dataframe containing L1C files
    df_L1C = pd.DataFrame(columns=['Date', 'Files'])
    L1C_dates = []
    L1C_files = []
    L1C_paths = []

    # Getting the path to the L1C files (for this specific tile)
    S2_L1C_tile_path = os.path.join(L1C_path, tile_name)

    # Initiate a list of all the L1C already available ...
    local_L1C = []

    # ... Fill it 
    for files in os.listdir(S2_L1C_tile_path):
        if "MSIL1C" in files and not ".zip" in files:
            L1C_files.append(re.sub('.SAFE', '', files, 1))
            L1C_paths.append(os.path.join(L1C_path, tile_name, re.sub('.SAFE', '', files, 1)))
            L1C_dates.append(int(files.split('_MSIL1C_')[1].split('T')[0]))

    df_L1C['Date'] = L1C_dates
    df_L1C['Files'] = L1C_files
    df_L1C['Paths'] = L1C_paths

    df_L1C = df_L1C.loc[~(df_L1C['Date'] < int(sta_date))]
    df_L1C = df_L1C.loc[~(df_L1C['Date'] >= int(end_date))]

    local_L1C = df_L1C['Paths'].tolist()

    # ... Return it
    return local_L1C


def get_missing(L1C_local, L2A_unconst, L2A_const, L1C_available, L1C_path, tile):
    missing_L2A = []
    missing_L1C = []

    L1C_dist_desc = [x.split("_MSIL1C_")[1].split("_N")[0] for x in L1C_available]
    L1C_loca_desc = [x.split("_MSIL1C_")[1].split("_N")[0] for x in L1C_local]
    L2A_con_desc = [x.split("_MSIL2A_")[1].split("_N")[0] for x in L2A_const]

    L1C_dict = dict(zip(L1C_dist_desc, L1C_available))

    for L1C_desc, L1C_files in zip(L1C_dist_desc, L1C_available):
        if L1C_desc not in L2A_con_desc:
            missing_L2A.append(L1C_desc)

    for missing in missing_L2A:
        if missing not in L1C_loca_desc:
            missing_L1C.append(L1C_dict[missing])
        else:
            is_consistant = consistancy_checker(L1C_path, L1C_dict[missing], tile)
            if is_consistant == False:
                missing_L1C.append(L1C_dict[missing])
                shutil.rmtree(os.path.join(L1C_path, tile, L1C_dict[missing]))

    return missing_L1C


def L1C_purge(L2A_local, L1C_local):
    L1C_to_remove = []

    # Get the S2 descriptors
    L2A_descriptors = [x.split("_MSIL2A_")[1].split("_N")[0] for x in L2A_local]
    L1C_descriptors = [x.split("_MSIL1C_")[1].split("_N")[0] for x in L1C_local]

    # Check for L1C files already processed as L2A
    for L1C_desc, L1C_files in zip(L1C_descriptors, L1C_local):
        if L1C_desc in L2A_descriptors:
            L1C_to_remove.append(L1C_files)

    # Purge 
    print("Purge : ")
    for L1C in L1C_to_remove:
        print("Removing " + L1C)


def L1C_download(L1C_files_to_download, L1C_available_json, L1C_dir, tile_name, attempts, tempo, peps_id, peps_pwd):
    os.environ["EODAG__PEPS__DOWNLOAD__OUTPUTS_PREFIX"] = os.path.join(L1C_dir, tile_name)
    os.environ["EODAG__PEPS__AUTH__CREDENTIALS__USERNAME"] = peps_id
    os.environ["EODAG__PEPS__AUTH__CREDENTIALS__PASSWORD"] = peps_pwd

    dag = EODataAccessGateway()
    dag.set_preferred_provider("peps")

    # We open it in order to get the EOProduct format (necessary to download the product.)
    deserialized_search_results = dag.deserialize(L1C_available_json)

    # This function allows to download the new L1C datas

    plugin_conf = {"extract": True}

    for products in deserialized_search_results:
        if products.as_dict()['id'] in L1C_files_to_download:
            print(f'Trying to download L1C product : ' + products.as_dict()['id'])
            # try :
            #    dag.download(products, **plugin_conf)
            # except :
            #    print("/!\ EODAG did not allow you to download this product : " + products)

            for i in range(attempts, 0, -1):
                try:
                    dag.download(products, **plugin_conf)

                except:
                    if i == 1:
                        raise
                    print("/!\ EODAG did not allow you to download the desired product")
                    print("Retry in " + str(tempo) + " seconds ... " + str(i) + " other requests will be done.")
                    time.sleep(int(tempo))

                else:
                    break
