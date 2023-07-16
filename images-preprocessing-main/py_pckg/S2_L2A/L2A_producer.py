import os as os
import pandas as pd
import sys as sys

sys.path.insert(0, '..')
from glob import glob


# Sen2Cor launcher
def process(input_dir, tile_name, output_dir, L1C_files):
    input = os.path.join(input_dir, tile_name)
    output = os.path.join(output_dir, tile_name)

    for files in L1C_files:
        print("-> Sen2Cor atmospheric & and geometric corrections launched for file " + files)

        to_process = glob(os.path.join(input, files, '*SAFE'))
        print(to_process)

        """
  
        #cmd = "docker run -v /mnt:/mnt atruffier/sen2cor --resolution 10 --GIP_L2A /mnt/tmp/Sen2Cor/L2A_GIPP.xml "
        cmd = "docker run -v /mnt:/mnt atruffier/sen2cor --resolution 10 " 
        cmd += to_process
        cmd += " --output_dir " + output

            
        print(cmd)

        os.system(cmd)
        """
