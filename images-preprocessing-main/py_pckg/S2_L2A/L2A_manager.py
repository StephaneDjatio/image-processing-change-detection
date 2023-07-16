from shlex import split

import pandas as pd

import os as os
from glob import glob

pd.set_option("display.max_rows", None, "display.max_columns", None)


class SENSOR:

    def __init__(self):

        self.tile = ""
        self.sensor = ""
        self.level = ""

        self.start = ""
        self.end = ""

        self.nodata = 0

        self.arbo_dict = {}
        self.acqDataframe = pd.DataFrame(columns=['acq_pathes', 'acq_dates', 'sensor', 'tile'])

    def arbo_printer(self):
        for keys in self.arbo_dict:
            print("\n ACQUISITION : " + keys)
            for keys2 in self.arbo_dict[keys]:
                print(keys2 + " : " + self.arbo_dict[keys][keys2])

    def arbo_printer(self):
        for keys in self.arbo_dict:
            print("\n ACQUISITION : " + keys)
            for keys2 in self.arbo_dict[keys]:
                print(keys2 + " : " + self.arbo_dict[keys][keys2])

    def csv_producer(self, indice):

        dates_list = []
        files_list = []
        indic_list = []
        senso_list = []

        for files in self.arbo_dict:
            if "MSIL2A" in files:
                dates_list.append(files.split("MSIL2A_")[1].split("T")[0])
                files_list.append(files)
                indic_list.append(indice)
                senso_list.append("S2")

        df = pd.DataFrame(columns=['Dates', 'Files', 'Indices', 'Sensors'])

        df['Dates'] = dates_list
        df['Files'] = files_list
        df['Indices'] = indic_list
        df['Sensors'] = senso_list

        return df


class L2A_MAJA(SENSOR):

    # Initialisation of rasters 
    def __init__(self, l2a_dir, tile_name, start_date, end_date):

        self.sensor = "S2"
        self.level = "L2A_MAJA"

        self.start = start_date
        self.end = end_date

        self.tile = tile_name
        self.nodata = -10000

        self.arbo_dict = {}
        self.acqDataframe = pd.DataFrame(columns=['acq_pathes', 'acq_dates', 'sensor'])

        self.acqList = []
        self.datesList = []

        for acquisitions in os.listdir(os.path.join(l2a_dir, self.tile)):

            if "SENTINEL2A" in acquisitions or "SENTINEL2B" in acquisitions:

                self.datesList.append(acquisitions.split('_')[1].split('-')[0])
                self.acqList.append(acquisitions)

                try:
                    B2_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B2.tif"))[0]
                except:
                    B2_band = "no_data"

                try:
                    B3_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B3.tif"))[0]
                except:
                    B3_band = "no_data"

                try:
                    B4_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B4.tif"))[0]
                except:
                    B4_band = "no_data"

                try:
                    B5_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B5.tif"))[0]
                except:
                    B5_band = "no_data"

                try:
                    B6_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B6.tif"))[0]
                except:
                    B6_band = "no_data"

                try:
                    B7_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B7.tif"))[0]
                except:
                    B7_band = "no_data"

                try:
                    B8_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B8.tif"))[0]
                except:
                    B8_band = "no_data"

                try:
                    B8A_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B8A.tif"))[0]
                except:
                    B8A_band = "no_data"

                try:
                    B11_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B11.tif"))[0]
                except:
                    B11_band = "no_data"

                try:
                    B12_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*FRE_B12.tif"))[0]
                except:
                    B12_band = "no_data"

                try:
                    MASK_10 = glob(os.path.join(l2a_dir, self.tile, acquisitions, "MASKS", "*CLM_R1.tif"))[0]
                except:
                    MASK_10 = "no_data"

                try:
                    MASK_20 = glob(os.path.join(l2a_dir, self.tile, acquisitions, "MASKS", "*CLM_R2.tif"))[0]
                except:
                    MASK_20 = "no_data"

                try:
                    EDG_R1 = glob(os.path.join(l2a_dir, self.tile, acquisitions, "MASKS", "*EDG_R1.tif"))[0]
                except:
                    EDG_R1 = "no_data"

                try:
                    EDG_R2 = glob(os.path.join(l2a_dir, self.tile, acquisitions, "MASKS", "*EDG_R2.tif"))[0]
                except:
                    EDG_R2 = "no_data"

                try:
                    QCK = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*ALL.jpg"))[0]
                except:
                    QCK = "no_data"

                try:
                    META = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*ALL.xml"))[0]
                except:
                    META = "no_data"

                bands = [B2_band, B3_band, B4_band, B5_band, B6_band, B7_band, B8_band, B8A_band, B11_band, B12_band,
                         MASK_10, MASK_20, EDG_R1, EDG_R2, QCK, META]
                names = ["BLUE", "GREEN", "RED", "veg_red_edg5", "veg_red_edg6", "veg_red_edg7", "NIR", "veg_red_edg8a",
                         "SWIR1", "SWIR2", "MASK", "MASKS20", "NO_DATA", "NO_DATA20", "QCK", "META"]

                df = pd.DataFrame(columns=['names', 'pathes'])

                df['names'] = names
                df['pathes'] = bands

                self.arbo_dict[acquisitions] = df

        self.acqDataframe['acq_pathes'] = self.acqList
        self.acqDataframe['acq_dates'] = self.datesList

        self.acqDataframe.sort_values(by=['acq_dates'], inplace=True)
        self.acqDataframe['acq_dates'] = self.acqDataframe['acq_dates'].astype(int)

        self.acqDataframe = self.acqDataframe[self.acqDataframe['acq_dates'] > int(start_date)]
        self.acqDataframe = self.acqDataframe[self.acqDataframe['acq_dates'] < int(end_date)]
        self.acqDataframe['sensor'] = self.sensor
        self.acqDataframe['tile'] = self.tile

        self.unconsistant_data = []
        self.consistant_data = []

        for idx, rows in self.acqDataframe.iterrows():
            if "no_data" in self.arbo_dict[rows['acq_pathes']]['pathes'].values:
                self.acqDataframe.drop(idx, inplace=True)
                self.unconsistant_data.append(rows['acq_pathes'])
            else:
                self.consistant_data.append(rows['acq_pathes'])

        if len(self.unconsistant_data) != 0:
            print("/ ! \ Warning :")
            print(
                "Some of your datas seem to be unconsistant ! They will not be considered in the future processings. Here is the list of unconsidered acquisitions : ")
            for acq in self.unconsistant_data:
                print("\n -> " + acq + " \n")
                print(self.arbo_dict[acq])
            print("/ ! \ End of Warning.")

        # UPDATE THE DISCTIONARY WITH THE CONSISTANT FILES + ORDER THE KEYS IN THE TEMPORAL EVOLUTION
        self.arbo_dict = {key: self.arbo_dict[key] for key in self.consistant_data}

        for key in self.arbo_dict:
            self.arbo_dict[key] = dict(zip(self.arbo_dict[key].names, self.arbo_dict[key].pathes))


class SEN2COR(SENSOR):

    # Initialisation of rasters 
    def __init__(self, l2a_dir, tile_name, start_date, end_date):

        self.sensor = "S2"
        self.level = "SEN2COR"

        self.start = start_date
        self.end = end_date

        self.tile = tile_name
        self.nodata = 0

        self.arbo_dict = {}
        self.acqDataframe = pd.DataFrame(columns=['acq_pathes', 'acq_dates', 'sensor'])

        self.acqList = []
        self.datesList = []

        for acquisitions in os.listdir(os.path.join(l2a_dir, self.tile)):

            if "S2A_MSIL2A" in acquisitions or "S2B_MSIL2A" in acquisitions and not ".zip" in acquisitions:

                self.datesList.append(acquisitions.split("MSIL2A_")[1].split('T')[0])
                self.acqList.append(acquisitions)

                try:
                    B2_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R10m",
                                                "*B02_10m.jp2"))[0]
                except:
                    B2_band = "no_data"

                try:
                    B3_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R10m",
                                                "*B03_10m.jp2"))[0]
                except:
                    B3_band = "no_data"

                try:
                    B4_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R10m",
                                                "*B04_10m.jp2"))[0]
                except:
                    B4_band = "no_data"

                try:
                    B5_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R20m",
                                                "*B05_20m.jp2"))[0]
                except:
                    B5_band = "no_data"

                try:
                    B6_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R20m",
                                                "*B06_20m.jp2"))[0]
                except:
                    B6_band = "no_data"

                try:
                    B7_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R20m",
                                                "*B07_20m.jp2"))[0]
                except:
                    B7_band = "no_data"

                try:
                    B8_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R10m",
                                                "*B08_10m.jp2"))[0]
                except:
                    B8_band = "no_data"

                try:
                    B8A_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R20m",
                                                 "*B8A_20m.jp2"))[0]
                except:
                    B8A_band = "no_data"

                try:
                    B11_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R20m",
                                                 "*B11_20m.jp2"))[0]
                except:
                    B11_band = "no_data"

                try:
                    B12_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R20m",
                                                 "*B12_20m.jp2"))[0]
                except:
                    B12_band = "no_data"

                try:
                    MASK_20 = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R20m",
                                                "*SCL_20m.jp2"))[0]
                except:
                    MASK_20 = "no_data"

                try:
                    MASK_60 = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R60m",
                                                "*SCL_60m.jp2"))[0]
                except:
                    MASK_60 = "no_data"

                try:
                    EDG_R1 = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R20m",
                                               "*SCL_20m.jp2"))[0]
                except:
                    EDG_R1 = "no_data"

                try:
                    EDG_R2 = glob(os.path.join(l2a_dir, self.tile, acquisitions, "GRANULE", "*", "IMG_DATA", "R20m",
                                               "*SCL_20m.jp2"))[0]
                except:
                    EDG_R2 = "no_data"

                try:
                    QCK = glob(os.path.join(l2a_dir, self.tile, acquisitions, "S2*.jpg"))[0]
                except:
                    QCK = "missing_qck"

                try:
                    META = glob(os.path.join(l2a_dir, self.tile, acquisitions, "MTD_MSIL2A.xml"))[0]
                except:
                    META = "no_data"

                bands = [B2_band, B3_band, B4_band, B5_band, B6_band, B7_band, B8_band, B8A_band, B11_band, B12_band,
                         MASK_20, MASK_60, EDG_R1, EDG_R2, QCK, META]
                names = ["BLUE", "GREEN", "RED", "veg_red_edg5", "veg_red_edg6", "veg_red_edg7", "NIR", "veg_red_edg8a",
                         "SWIR1", "SWIR2", "MASK", "MASKS20", "NO_DATA", "NO_DATA20", "QCK", "META"]
                df = pd.DataFrame(columns=['names', 'pathes'])

                df['names'] = names
                df['pathes'] = bands

                self.arbo_dict[acquisitions] = df

        self.acqDataframe['acq_pathes'] = self.acqList
        self.acqDataframe['acq_dates'] = self.datesList

        self.acqDataframe.sort_values(by=['acq_dates'], inplace=True)
        self.acqDataframe['acq_dates'] = self.acqDataframe['acq_dates'].astype(int)

        self.acqDataframe = self.acqDataframe[self.acqDataframe['acq_dates'] >= int(start_date)]
        self.acqDataframe = self.acqDataframe[self.acqDataframe['acq_dates'] < int(end_date)]
        self.acqDataframe['sensor'] = self.sensor
        self.acqDataframe['tile'] = self.tile

        self.unconsistant_data = []
        self.consistant_data = []

        for idx, rows in self.acqDataframe.iterrows():
            if "no_data" in self.arbo_dict[rows['acq_pathes']]['pathes'].values:
                self.acqDataframe.drop(idx, inplace=True)
                self.unconsistant_data.append(rows['acq_pathes'])
            else:
                self.consistant_data.append(rows['acq_pathes'])

        if len(self.unconsistant_data) != 0:
            print("/ ! \ Warning :")
            print(
                "Some of your datas seem to be unconsistant ! They will not be considered in the future processings. Here is the list of unconsidered acquisitions : ")
            for acq in self.unconsistant_data:
                print("\n -> " + acq + " \n")
                print(self.arbo_dict[acq])
            print("/ ! \ End of Warning.")

        # UPDATE THE DISCTIONARY WITH THE CONSISTANT FILES + ORDER THE KEYS IN THE TEMPORAL EVOLUTION
        self.arbo_dict = {key: self.arbo_dict[key] for key in self.consistant_data}
        for key in self.arbo_dict:
            self.arbo_dict[key] = dict(zip(self.arbo_dict[key].names, self.arbo_dict[key].pathes))


class LANDSAT8(SENSOR):

    # Initialisation of rasters 
    def __init__(self, l2a_dir, tile_name, start_date, end_date):

        self.sensor = "LANDSAT8"
        self.level = "L2"

        self.start = start_date
        self.end = end_date

        self.tile = tile_name

        self.arbo_dict = {}
        self.acqDataframe = pd.DataFrame(columns=['acq_pathes', 'acq_dates', 'sensor'])

        self.acqList = []
        self.datesList = []

        for acquisitions in os.listdir(os.path.join(l2a_dir, self.tile)):

            if ("LC08_L2SP_" in acquisitions and not ".tar" in acquisitions) or (
                    "LC09_L2SP_" in acquisitions and not ".tar" in acquisitions):

                self.datesList.append(acquisitions.split("L2SP_" + self.tile + "_")[1].split('_')[0])
                self.acqList.append(acquisitions)

                try:
                    B1_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*_SR_B1.TIF"))[0]
                except:
                    B1_band = "no_data"

                try:
                    B2_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*_SR_B2.TIF"))[0]
                except:
                    B2_band = "no_data"

                try:
                    B3_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*_SR_B3.TIF"))[0]
                except:
                    B3_band = "no_data"

                try:
                    B4_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*_SR_B4.TIF"))[0]
                except:
                    B4_band = "no_data"

                try:
                    B5_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*_SR_B5.TIF"))[0]
                except:
                    B5_band = "no_data"

                try:
                    B6_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*_SR_B6.TIF"))[0]
                except:
                    B6_band = "no_data"

                try:
                    B7_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*_SR_B7.TIF"))[0]
                except:
                    B7_band = "no_data"

                try:
                    B10_band = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*_ST_B10.TIF"))[0]
                except:
                    B10_band = "no_data"

                try:
                    MASK_30 = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*QA_PIXEL.TIF"))[0]
                except:
                    MASK_30 = "no_data"

                try:
                    QCK = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*_thumb_large.jpeg"))[0]
                except:
                    QCK = "missing_qck"

                try:
                    META = glob(os.path.join(l2a_dir, self.tile, acquisitions, "*T1_MTL.xml"))[0]
                except:
                    META = "no_meta"

                bands = [B1_band, B2_band, B3_band, B4_band, B5_band, B6_band, B7_band, MASK_30, QCK, META]
                names = ["coast_aer", "BLUE", "GREEN", "RED", "NIR", "SWIR1", "SWIR2", "MASK", "QCK", "META"]

                df = pd.DataFrame(columns=['names', 'pathes'])

                df['names'] = names
                df['pathes'] = bands

                self.arbo_dict[acquisitions] = df

        self.acqDataframe['acq_pathes'] = self.acqList
        self.acqDataframe['acq_dates'] = self.datesList

        self.acqDataframe.sort_values(by=['acq_dates'], inplace=True)
        self.acqDataframe['acq_dates'] = self.acqDataframe['acq_dates'].astype(int)

        self.acqDataframe = self.acqDataframe[self.acqDataframe['acq_dates'] > int(start_date)]
        self.acqDataframe = self.acqDataframe[self.acqDataframe['acq_dates'] <= int(end_date)]
        self.acqDataframe['sensor'] = self.sensor
        self.acqDataframe['tile'] = self.tile

        self.unconsistant_data = []
        self.consistant_data = []

        for idx, rows in self.acqDataframe.iterrows():
            if "no_data" in self.arbo_dict[rows['acq_pathes']]['pathes'].values:
                self.unconsistant_data.append(rows['acq_pathes'])
                self.acqDataframe.drop(idx, inplace=True)
            else:
                self.consistant_data.append(rows['acq_pathes'])

        if len(self.unconsistant_data) != 0:
            print("/ ! \ Warning :")
            print(
                "Some of your datas seem to be unconsistant ! They will not be considered in the future processings. Here is the list of unconsidered acquisitions : ")
            for acq in self.unconsistant_data:
                print("\n -> " + acq + " \n")
                print(self.arbo_dict[acq])
            print("/ ! \ End of Warning.")

        # UPDATE THE DISCTIONARY WITH THE CONSISTANT FILES + ORDER THE KEYS IN THE TEMPORAL EVOLUTION
        self.arbo_dict = {key: self.arbo_dict[key] for key in self.consistant_data}
        for key in self.arbo_dict:
            self.arbo_dict[key] = dict(zip(self.arbo_dict[key].names, self.arbo_dict[key].pathes))


def get_missing(L2A_local, L1C_available):
    missing_L2A = []

    # Get the S2 descriptors
    L2A_descriptors = [x.split("_MSIL2A_")[1].split("_N")[0] for x in L2A_local]
    L1C_descriptors = [x.split("_MSIL1C_")[1].split("_N")[0] for x in L1C_available]

    # Finding the L1C that are available on PEPS and not already downloaded
    for L1C_desc, L1C_files in zip(L1C_descriptors, L1C_available):
        if L1C_desc not in L2A_descriptors:
            missing_L2A.append(L1C_files)

    return missing_L2A


# Sen2Cor launcher
def production(input_dir, tile_name, output_dir, L1C_files, unconsistant_L2A_list):
    input = os.path.join(input_dir, tile_name)
    output = os.path.join(output_dir, tile_name)

    if len(unconsistant_L2A_list) > 0:
        print("-> Removing inconsistant data, for Sen2Cor relaunch")

    for unconsistant_L2A in unconsistant_L2A_list:
        unconsistant_L2A_path = os.path.join(output, unconsistant_L2A)
        cmd = "rm -r " + unconsistant_L2A_path
        print(cmd)
        os.system(cmd)

    for files in L1C_files:
        print("\n PROCESS || Sen2Cor atmospheric & and geometric corrections launched for file " + files + "\n")

        to_process = glob(os.path.join(files, "*.SAFE"))[0]

        cmd = "/Users/pro/Desktop/Sen2Cor-02.10.01-Darwin64/bin/L2A_Process --resolution 10 "
        cmd += "~/Desktop/Projets/snornf/images-preprocessing-main/" + to_process
        cmd += " --output_dir ~/Desktop/Projets/snornf/images-preprocessing-main/" + output

        print(cmd)

        os.system(cmd)
