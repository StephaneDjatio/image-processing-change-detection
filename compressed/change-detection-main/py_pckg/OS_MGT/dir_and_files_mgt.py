import os  

# This python file contains all the functions allowing to manage files and folders

def create_dir_tiles(path, tile):

    dir_name = os.path.join(path, tile)
    
    if not os.path.exists(dir_name) : 
        os.makedirs(dir_name)