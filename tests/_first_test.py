# This file is used to import the root folder of the project to the sys.path list. the filename \
# needs to be first alphabetically so that pytest updates the path for all subsequent tests

import sys
import pathlib

# add the path of the root folder to the sys.path list
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))