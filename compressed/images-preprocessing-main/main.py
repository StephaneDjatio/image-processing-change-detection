import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

import py_pckg.S2_L1C as S2_L1C
import py_pckg.S2_L2A as S2_L2A
import py_pckg.TIME   as TIME
import py_pckg.OS_MGT as OS_MGT
import py_pckg.L8     as L8

import configparser
import json
import os 

import time
start_time = time.time()

################################################################################################################################################################################################
#                                                                                                                                                                                              #
#                                                                                                                                                                                              #
#                                                                    S N O R N F : Pre-Processing chain                                                                                        #
#                                                                                                                                                                                              #
#                                                                                                                                                                                              #
################################################################################################################################################################################################

print("\n\n\n\n                                          * ** ***  S N O R N F *** ** *                                       ")  

print("\n\n\n                                       ||  Pre-Processing chain launched ! ||    \n\n\n  ")


# global vars
current_main_dir = os.path.dirname(os.path.realpath(__file__))

# Get the headers from the user config file
print("\n INFO || Parsing the user options... \n")
configParser = configparser.ConfigParser() 
configFilePath = f'{current_main_dir}/config/config.cfg'
configParser.read(configFilePath, encoding='utf-8')

user_pathes       = configParser["PATHES"]
user_credentials  = configParser["CREDENTIALS"]
user_geoinfos     = configParser["GEOINFOS"]
user_perdiod      = configParser["PERIOD"]
user_options      = configParser["OPTIONS"]
user_pathes_s     = configParser["PATHES_SENSOR"]

# Dates management 
start_date, end_date = TIME.get_time_period(user_perdiod["monthes"])
start_date, end_date = "20220101", "20230501"
print("\n INFO || The Pre-Processing chain is launched on the " + start_date + "-" + end_date + " period. \n")

# Read the S2 tiles
S2_tile_list = json.loads(user_geoinfos["S2_tiles"])



for tiles in S2_tile_list : 

    print(" >    S E N T I N E L   2   :  P R E - P R O C E S S I N G ")

    print("\n INFO || S2 Tile : "+ tiles +" \n")

    # Initiate directories for L1C and L2A pool 
    OS_MGT.create_dir_tiles(user_pathes_s["L1C_pool"], tiles)
    OS_MGT.create_dir_tiles(user_pathes_s["L2A_pool"], tiles)

    # -> List existing L2A files, then look for unconsistant files (missing bands, etc)
    S2_L2A_S2C_obj     = S2_L2A.SEN2COR(user_pathes_s["L2A_pool"], tiles, start_date, end_date)
    L2A_existing_files = S2_L2A_S2C_obj.consistant_data
    L2A_corrupte_files = S2_L2A_S2C_obj.unconsistant_data

    # -> List L1C files that could be downloaded, and download the interesting ones
    L1C_files_available = S2_L1C.search(tiles, start_date, end_date, f'{current_main_dir}' + user_pathes["L1C_files"], user_credentials['peps_id'], user_credentials['peps_pwd'])
    L1C_files_local     = S2_L1C.get_local(tiles, start_date, end_date, user_pathes_s["L1C_pool"])
    L1C_to_download     = S2_L1C.get_missing(L1C_files_local, L2A_corrupte_files, L2A_existing_files, L1C_files_available, user_pathes_s["L1C_pool"], tiles)

    S2_L1C.L1C_download(L1C_to_download, f'{current_main_dir}' + user_pathes["L1C_files"], user_pathes_s["L1C_pool"], tiles, int(user_options["peps_attempts"]), user_options["peps_tempo"], user_credentials['peps_id'], user_credentials['peps_pwd'])    
    """
    # -> Produce missing L2A 
    S2_L2A_S2C_obj     = S2_L2A.SEN2COR(user_pathes_s["L2A_pool"], tiles, start_date, end_date)
    L2A_existing_files = S2_L2A_S2C_obj.consistant_data
    L2A_corrupte_files = S2_L2A_S2C_obj.unconsistant_data
    
    L1C_files_local    = S2_L1C.get_local(tiles, start_date, end_date, user_pathes_s["L1C_pool"])
    L2A_to_produce     = S2_L2A.get_missing(L2A_existing_files, L1C_files_local)

    S2_L2A.production(user_pathes_s["L1C_pool"], tiles, user_pathes_s["L2A_pool"], L2A_to_produce, L2A_corrupte_files)

    
    # -> Delete L1C that have already been processed in L2A_Sen2Cor
    if  user_options["purge"].upper() == "TRUE" : 

        S2_L2A_S2C_obj = S2_L2A.SEN2COR(user_pathes_s["L2A_pool"], tiles, start_date, end_date)
        
        L2A_existing_files = S2_L2A_S2C_obj.consistant_data
        L1C_existing_files = S2_L1C.check(tiles,start_date, end_date, user_pathes_s["L1C_pool"])
        L1C_to_purge       = S2_L1C.L1C_purge(L2A_existing_files, L1C_existing_files)
    




    # -> Download corresponding L8
    print(" >    L A N D S A T 8 / 9   :  P R E - P R O C E S S I N G ")

    if  user_options["landsat_activation"].upper() == "TRUE" : 

        L8_tiles = L8.L8_corresponding_to_S2(tiles, current_main_dir + user_pathes["S2_shapes"], current_main_dir + user_pathes["L8_shapes"])
        
        for tile in L8_tiles : 

            print("\n INFO || Corresponding Landsat Tile in process : "+ tile +" \n")
            
            OS_MGT.create_dir_tiles(user_pathes_s["L8_pool"], tile)
            
            csv_availabale_landsat    = L8.list_available_landsat("landsat_ot_c2_l2", tile, start_date, end_date, user_pathes_s["L8_pool"], user_credentials["landsat_explorer_id"],user_credentials["landsat_explorer_password"], 90, current_main_dir + user_pathes["L8_shapes"])
            
            
            L8_obj = S2_L2A.LANDSAT8(user_pathes_s["L8_pool"], tile, start_date, end_date)
            
            L8_existing_files = L8_obj.consistant_data
            L8_corrupte_files = L8_obj.unconsistant_data
            
            
            landsat_to_download_list = L8.get_missing(csv_availabale_landsat, L8_existing_files, L8_corrupte_files)
        
            L8.download_landsat(landsat_to_download_list, L8_corrupte_files, user_credentials["landsat_explorer_id"], user_credentials["landsat_explorer_password"], user_pathes_s["L8_pool"], tile)
            
            L8_obj = S2_L2A.LANDSAT8(user_pathes_s["L8_pool"], tile, start_date, end_date)
            """        
    
   
print("\n INFO || %s Processing ---" % (time.time() - start_time))



