import os
from os import listdir
from os.path import join

#Dataset path, TODO: Move this to a config file so it can be changed
path = "D:\\Documents\\Python Projects\\formula-1-analytics\\data\\external"

def read_datasets():
    print("Printing dataset names")
    files = os.listdir(path)

    file = open(join(path,files[0]),"r")
    print(file.read())

