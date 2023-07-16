import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

import py_pckg.S2_L2A as S2_L2A
import py_pckg.TIME as TIME
import py_pckg.CG_DET as CG_DET
import py_pckg.RASTER as RASTER
import py_pckg.VECTOR as VECTOR
import py_pckg.L8 as L8
import py_pckg.SQL as SQL
import configparser
import json
import os

import time

start_time = time.time()

import pandas as pd

################################################################################################################################################################################################
#                                                                                                                                                                                              #
#                                                                                                                                                                                              #
#                                                                    S N O R N F : Change Detection chain                                                                                        #
#                                                                                                                                                                                              #
#                                                                                                                                                                                              #
################################################################################################################################################################################################

print(
    "\n\n\n\n                                          * ** ***  S N O R N F *** ** *                                       ")

print("\n\n\n                                       ||  Change Detection chain launched ! ||     \n\n\n")

# global vars
current_main_dir = os.path.dirname(os.path.realpath(__file__))

# Get the headers from the user config file
configParser = configparser.ConfigParser()
configFilePath = f'{current_main_dir}/config/config.cfg'
configParser.read(configFilePath, encoding='utf-8')

# Creating pathes if not existing yet
for section in configParser.sections():
    if section == "PATHES_PROCESS":
        for (each_key, each_val) in configParser.items(section):
            if "." not in each_val and not "data" in each_val:
                if not os.path.exists(current_main_dir + each_val):
                    os.makedirs(current_main_dir + each_val)
            elif "data" in each_val:
                if not os.path.exists(each_val):
                    os.makedirs(each_val)

user_pathes_s = configParser["PATHES_SENSOR"]
user_pathes_p = configParser["PATHES_PROCESS"]
user_geoinfos = configParser["GEOINFOS"]
user_perdiod = configParser["PERIOD"]
user_options = configParser["OPTIONS"]
user_params = configParser["PARAMS"]
user_patter = configParser["PATTERN"]
user_database = configParser["DATABASE"]

# Dates management 
start_date, end_date = TIME.get_time_period(user_perdiod["monthes"])
start_date, end_date = "20220301", "20220601"

# Read the S2 tiles
S2_tile_list = json.loads(user_geoinfos["S2_tiles"])

chg_vector_list = []

# S2 Processing part
for S2_tile in S2_tile_list:

    print(
        "\n INFO || " + S2_tile + " S2 tile process starting : creating the dataframe which will contain all the acquisitions. \n")
    # -> List existing L2A files, then look for unconsistant files (missing bands, etc)

    S2_L2A_S2C_obj = S2_L2A.SEN2COR(user_pathes_s["L2A_pool"], S2_tile, start_date, end_date)

    L2A_existing_files = S2_L2A_S2C_obj.consistant_data
    L2A_corrupte_files = S2_L2A_S2C_obj.unconsistant_data

    # L8 Processing part
    L8_acq_list = []

    if user_options["landsat_activation"].upper() == "TRUE":

        L8_tiles = L8.L8_corresponding_to_S2(S2_tile, current_main_dir + user_pathes_p["S2_shapes"],
                                             current_main_dir + user_pathes_p["L8_shapes"])

        for L8_tile in L8_tiles:
            print(
                "\n INFO || " + L8_tile + " Landsat tile process starting : creating the dataframes which will contain all the acquisitions. \n")

            L8_obj = S2_L2A.LANDSAT8(user_pathes_s["L8_pool"], L8_tile, start_date, end_date)
            L8_acq_list.append(L8_obj)

    target_shape = RASTER.get_target_shape(S2_L2A_S2C_obj.arbo_dict[next(iter(S2_L2A_S2C_obj.arbo_dict))]["RED"])
    dates, changes, acq_datf_S2_L8 = CG_DET.process(S2_L2A_S2C_obj, L8_acq_list, target_shape,
                                                    current_main_dir + user_pathes_p["S2_shapes"],
                                                    current_main_dir + user_pathes_p["tmp"], S2_tile,
                                                    user_params["S2_bot_threshold"], user_params["S2_top_threshold"],
                                                    user_params["L8_bot_threshold"], user_params["L8_top_threshold"],
                                                    S2_L2A_S2C_obj.arbo_dict[next(iter(S2_L2A_S2C_obj.arbo_dict))][
                                                        "RED"], int(user_params["max_change_size_per_detect"]))

    csv_path = os.path.join(current_main_dir + "/" + user_pathes_p['chg_csv'],
                            user_patter['chg_pat_csv'] + "_" + S2_tile + "_" + str(start_date) + "_" + str(
                                end_date) + ".csv")

    print(current_main_dir + user_pathes_p['chg_csv'])

    chg_path_ras = os.path.join(current_main_dir + "/" + user_pathes_p['chg_rast'],
                                user_patter['chg_pat_ras'] + "_" + S2_tile + "_" + str(start_date) + "_" + str(
                                    end_date) + ".tif")
    dat_path_ras = os.path.join(current_main_dir + "/" + user_pathes_p['dat_rast'],
                                user_patter['dat_pat_ras'] + "_" + S2_tile + "_" + str(start_date) + "_" + str(
                                    end_date) + ".tif")

    chg_path_vec = os.path.join(current_main_dir + "/" + user_pathes_p['chg_vec'],
                                user_patter['chg_pat_vec'] + "_" + S2_tile + "_" + str(start_date) + "_" + str(
                                    end_date) + ".shp")
    dat_path_vec = os.path.join(current_main_dir + "/" +  user_pathes_p['chg_vec'],
                                user_patter['dat_pat_vec'] + "_" + S2_tile + "_" + str(start_date) + "_" + str(
                                    end_date) + ".shp")

    merged_vec = os.path.join(current_main_dir + "/" + user_pathes_p['chg_dat_vec'],
                              user_patter['chg_pat_vec'] + "_" + S2_tile + "_" + str(start_date) + "_" + str(
                                  end_date) + ".shp")

    acq_datf_S2_L8.to_csv(csv_path)

    print("\n INFO || " + S2_tile + " occurences and dates obtained saving starts \n")

    RASTER.writter(S2_L2A_S2C_obj.arbo_dict[next(iter(S2_L2A_S2C_obj.arbo_dict))]["RED"], changes, chg_path_ras)
    RASTER.writter(S2_L2A_S2C_obj.arbo_dict[next(iter(S2_L2A_S2C_obj.arbo_dict))]["RED"], dates, dat_path_ras)

    print("\n INFO || " + S2_tile + " occurences and dates obtained cleaning starts \n")

    chg_path_ras, dat_path_ras = RASTER.clean(chg_path_ras, dat_path_ras, chg_path_ras, dat_path_ras,
                                              int(user_params["min_size_changes"]),
                                              int(user_params["min_occu_changes"]),
                                              current_main_dir + "/" + user_pathes_p['forest_msk'],
                                              current_main_dir + "/" + user_pathes_p['S2_shapes'],
                                              int(user_params["forest_mask_tampon"]), S2_tile,
                                              user_params["lc_code_forest_mask"])
    print("\n INFO || " + S2_tile + " occurences and dates convertion to vectors starts \n")

    RASTER.raster_to_vector(chg_path_ras, chg_path_vec, user_patter["chg_attribute"])
    RASTER.raster_to_vector(dat_path_ras, dat_path_vec, user_patter["dat_attribute"])

    print("\n INFO || " + S2_tile + " merge of occurences and dates vectors starts \n")

    VECTOR.merge_vectors(chg_path_vec, user_patter["chg_attribute"], dat_path_vec, user_patter["dat_attribute"],
                         merged_vec)

    print("\n INFO || " + S2_tile + " filling with change detection infos starts \n")

    VECTOR.fill_vector(merged_vec, csv_path, merged_vec)

    chg_vector_list.append(merged_vec)

print("\n INFO || " + S2_tile + " save of final vector starts \n")

result_vec = os.path.join(user_pathes_p['chg_dat_vec_f'],
                          user_patter['chg_pat_vec'] + "_GABON_" + str(start_date) + "_" + str(end_date) + ".shp")

VECTOR.combine_shapefiles(chg_vector_list, result_vec)

"""
print("\n INFO || Uploading the final vector to the geodatabase. \n")

SQL.drop_table(user_database["user"], user_database["password"], user_database["host"], int(user_database["port"]), user_database["database"], user_database["table_name"])
SQL.push_table(result_vec, user_database["user"], user_database["password"], user_database["host"], int(user_database["port"]), user_database["database"], user_database["table_name"])
"""
print("--- %s seconds ---" % (time.time() - start_time))
