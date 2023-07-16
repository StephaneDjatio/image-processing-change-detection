import geopandas as gpd
import pandas as pd 
import fiona
from shapely.geometry import Polygon
import os as os 
from eodag import EODataAccessGateway
import json
from landsatxplore.api import API
from landsatxplore.earthexplorer import EarthExplorer




def L8_corresponding_to_S2(tile_name, gabon_shapes_S2, gabon_shapes_LS) :

    corresponding_LS = []

    with fiona.open(gabon_shapes_S2, "r") as shapefile:
            for f in shapefile:
                if f['properties']['Name'] == tile_name : 
                    p1 = f['geometry']['coordinates']
                    p1 =Polygon(f['geometry']['coordinates'][0])

                    with fiona.open(gabon_shapes_LS, "r") as shapefile:
                        for f in shapefile:
                            p2 = f['geometry']['coordinates']
                            p2 = Polygon(f['geometry']['coordinates'][0])

                            if(p1.intersects(p2)) :
                                corresponding_LS.append(f['properties']['WRSPR'])

    return corresponding_LS

def landsat_downloader(level, wrspr_code, sta_date, end_date, landsat_path, username, password, gabon_shapes_LS):

    output_dir_tile = os.path.join(landsat_path, wrspr_code)
    if not os.path.exists(output_dir_tile) : 
        os.makedirs(output_dir_tile)

    # Initialize a new API instance and get an access key
    api = API("atruffier", "GroupCLS__59")
    landsat_tiles = gpd.GeoDataFrame.from_file(gabon_shapes_LS)
    landsat_selected = landsat_tiles[landsat_tiles['WRSPR']==wrspr_code]

    # Changing date format
    sta = str(sta_date)[0:4] + "-" + str(sta_date)[4:6] + "-" + str(sta_date)[6:8]
    end = str(end_date)[0:4] + "-" + str(end_date)[4:6] + "-" + str(end_date)[6:8]

    for index, row in landsat_selected.iterrows():
        lon = row.geometry.centroid.x
        lat = row.geometry.centroid.y

    # Search for Landsat TM scenes
    scenes = api.search(
        dataset=level,
        latitude=lat,
        longitude=lon,
        start_date=sta,
        end_date=end,
    )

    print(f"{len(scenes)} scenes found.")

    # Process the result
    for scene in scenes:
        print(scene['entity_id'])

        cmd = "landsatxplore download -u " + username + " -p "+ password + " " + scene['entity_id'] + " -o " + output_dir_tile
        print(cmd)
        os.system(cmd)

    api.logout()


def list_available_landsat(level, wrspr_code, start_date, end_date, landsat_path, username, password, cloud_thresh, gabon_shapes_LS):

    print(f'-> Checking for new acquisitions : asking for availables L8/L9 (Tile : {wrspr_code}, Period : {start_date} - {end_date})')
    
    landsat_tiles = gpd.GeoDataFrame.from_file(gabon_shapes_LS)
    
    landsat_selected = landsat_tiles[landsat_tiles['WRSPR']==wrspr_code]

    for index, row in landsat_selected.iterrows():
        lon = row.geometry.centroid.x
        lat = row.geometry.centroid.y


    csv_tmp = os.path.join(landsat_path,wrspr_code) + "_" + start_date + "_" + end_date + "_" + str(cloud_thresh) + ".csv"
    cmd = "landsatxplore search -u "+username+" -p "+ password+" --dataset "+level+" --location "+ str(lat) +" "+ str(lon) +" -m 50000 --start "+start_date+" --end "+end_date + " > " + csv_tmp

    os.system(cmd)

    return csv_tmp 


    
def list_available_landsat2(tile_name, sta_date, end_date, json_available_LS, L8_path):

    output_dir = os.path.join(L8_path, tile_name)
    if not os.path.exists(output_dir) : 
        os.makedirs(output_dir)
    
    os.environ["EODAG__USGS__DOWNLOAD__OUTPUTS_PREFIX"] = os.path.join(L8_path, tile_name)
    
    landsat_tiles = gpd.GeoDataFrame.from_file("/home/atruffier/projects/LANDSAT_DL/WRS2_descending.shp")
    
    landsat_selected = landsat_tiles[landsat_tiles['WRSPR']==tile_name]

    for index, row in landsat_selected.iterrows():
        lon = row.geometry.centroid.x
        lat = row.geometry.centroid.y

    # Changing date format
    sta = str(sta_date)[0:4] + "-" + str(sta_date)[4:6] + "-" + str(sta_date)[6:8]
    end = str(end_date)[0:4] + "-" + str(end_date)[4:6] + "-" + str(end_date)[6:8]

    # Opening our gateway and setting PEPS as our provider
    dag = EODataAccessGateway()
    dag.set_preferred_provider("usgs")
    
    # Setting our search criterias
    search_criteria = {
    "productType": "landsat_ot_c2_l2",
    "start": sta,
    "end": end,
    "geom" :{'lonmin': lon, 'latmin': lat, 'lonmax': lon, 'latmax': lat}
    }

    # Launching the search
    products_found = dag.search_all(**search_criteria)
    dag.download_all(products_found)
    

    # Saving the result as a json file
    """
    dag.serialize(products_found, filename=json_available_LS)
    
    # Opening the json file
    file_available_LS = open(json_available_LS)
    LS_json_content   = json.load(file_available_LS)

    # Initiate the list containing the names of the available L1C files...
    LS_available = []

    #... and adding the L1C file names to this list
    for vars in LS_json_content['features'] : 
        LS_available.append(vars['id'])
    """
    return "opl"


def get_missing(availabale_landsat, L8_existing_files, L8_corrupte_files) :

    
    available_L8 = pd.read_csv(availabale_landsat, header=None,sep=";").iloc[:, 0].tolist()

    missing = []
    
    # Get the S2 descriptors
    LS_ex_descriptors = [x for x in L8_existing_files]
    LS_av_descriptors = [x for x in available_L8]


    # Finding the L1C that are available on PEPS and not already downloaded
    for availables in LS_av_descriptors :
        if availables not in LS_ex_descriptors : 
            missing.append(availables)

    # Finding the L1C that are available on PEPS and not already downloaded
    for availables in LS_av_descriptors :
        if availables in L8_corrupte_files : 
            missing.append(availables)

    missing = list(set(missing))
    
    return missing



def download_landsat(landsat_list, L8_corrupte_files, username, password, output_path, tile_name):

    output_dir = os.path.join(output_path, tile_name)
    if not os.path.exists(output_dir) : 
        os.makedirs(output_dir)

    for landsat_files in landsat_list : 

        if ("LC08" in landsat_files) or ("LC09" in landsat_files) :
        
            try :
              cmd = "landsatxplore download -u " + username + " -p "+ password + " " + landsat_files + " -o " + output_dir
              os.system(cmd)
              
              if not os.path.exists(os.path.join(output_dir, landsat_files)) : 
                  os.makedirs(os.path.join(output_dir, landsat_files))
              
              cmd = "tar -xvf "  + os.path.join(output_dir, landsat_files) +".tar -C "+ os.path.join(output_dir, landsat_files)
              print(cmd)
              os.system(cmd)
              os.system("rm -r " + os.path.join(output_dir, landsat_files) +".tar")
            except : 
              print("/!\ Warning : the USGS website seems not reachable.")
            
    for landsat_files in L8_corrupte_files : 
            
            if landsat_files not in landsat_list :
                try :
                  cmd = "landsatxplore download -u " + username + " -p "+ password + " " + landsat_files + " -o " + output_dir
                  os.system(cmd)
                  
                  
                  if not os.path.exists(os.path.join(output_dir, landsat_files)) : 
                      os.makedirs(os.path.join(output_dir, landsat_files))
                  
                  cmd = "tar -xvf "  + os.path.join(output_dir, landsat_files) +".tar -C "+ os.path.join(output_dir, landsat_files)
                  print("Running : " + cmd)
                  os.system(cmd)
                  os.system("rm -r " + os.path.join(output_dir, landsat_files) +".tar")
                except : 
                  print("/!\ Warning : the USGS website seems not reachable.")
                


   
def LS_download(LS_files_to_download, LS_available_json, LS_dir, tile_name):

    output_dir = os.path.join(LS_dir, tile_name)
    if not os.path.exists(output_dir) : 
        os.makedirs(output_dir)

    os.environ["EODAG__USGS__DOWNLOAD__OUTPUTS_PREFIX"] = os.path.join(LS_dir, tile_name)

    dag = EODataAccessGateway()
    dag.set_preferred_provider("usgs")

    deserialized_search_results = dag.deserialize(LS_available_json)
    for LS in deserialized_search_results : 
        dag.download_all([deserialized_search_results])


    """
    # We open it in order to get the EOProduct format (necessary to download the product.)
    deserialized_search_results = dag.deserialize(LS_available_json)

    # This function allows to download the new LS datas

    plugin_conf = {"extract": True, "products" : "LANDSAT_C2L2"}

    for productss in  deserialized_search_results : 
        if productss.as_dict()['id'] in LS_files_to_download : 
            print(f'Downloadind Landsat product : ' + productss.as_dict()['id'])
            dag.download(productss, **plugin_conf)
    """