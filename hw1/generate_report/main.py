import pandas as pd
import os

# define the path of data directory 
data_directory = '../processed_data/'

# for combinition of these columns
columns_path = []
columns_name = []
columns_type = {}

# remove the unrelated data
for dir in os.listdir(data_directory):
    datapath = os.path.join(data_directory, dir)
    if os.path.isdir(datapath):
        columns_path.append(datapath)
        columns_name.append(dir)
        columns_type[dir] = float

# different columns to combine
df = pd.concat((pd.read_csv(col+'/part-00000', dtype=columns_type) for col in columns_path), axis='columns')
df.columns = columns_name

# output the report that processed by spark cluster
outputpath = './reports'
df.to_csv(outputpath+'/output.csv', index=False)
print('finish to write out')