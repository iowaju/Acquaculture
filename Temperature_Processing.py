from netCDF4 import Dataset
from datetime import datetime
import numpy as np
import pandas as pd
import csv
import os
import datetime
import time

directory = r"C:/Ocean/Chile/1_temperature/"
reference = pd.read_csv('C:/Ocean/Chile/reference/Temperature_Georref.csv')

def ___Processing___():
    df_concat = pd.DataFrame()
    for filename in os.listdir(directory + "raw_files/"):
        if filename.endswith("nc"):
                dataset = Dataset(directory + "raw_files/" + filename)
                date = filename[0:7]
                data = datetime.datetime.strptime(str(date), '%Y%j').strftime('%Y/%m/%d')
                var = dataset.variables['sst4'][:][:]
                
                m, n = var.shape
                R, C = np.mgrid[:m, :n]
                out = np.column_stack((C.ravel()[22500000:35000000],
                                       R.ravel()[22500000:35000000],
                                       var.ravel()[22500000:35000000]))
                df1 = pd.DataFrame(out)
                dataset.close()
                df1.columns = ['long', 'lat', 'var']
                df = df1.loc[df1['var'] > 0]

                df['lat'] = df['lat'] * 0.041666668
                df['long'] = df['long'] * 0.041666668
              
                df['lat'] = df['lat'] - 90
                df['lat'] = df['lat'] * (-1)
                df['long'] = df['long'] - 180
                
                df = df.drop(df[df.lat < -55.2].index) #North
                df = df.drop(df[df.lat > -40.5].index) #South
                df = df.drop(df[df.long > -67].index) #East
                df = df.drop(df[df.long < -75].index) #Westdf

                df['long'] = df['long'].map(lambda x: '%2.8f' % x)
                df['lat'] = df['lat'].map(lambda x: '%2.8f' % x)

                df['data'] = data

                df['Unique'] = df['long'].map(str) + '-' + df['lat'].map(str)
                df.drop(['long'], axis = 1, inplace = True)
                df.drop(['lat'], axis = 1, inplace = True)

                df_concat = pd.concat([df_concat, df])
                print data

    df_merged = pd.merge(reference, df_concat, left_on = 'Lat_Lon', right_on = "Unique", how = 'left')
    df_merged.rename(columns = {'data' : 'date', 'var' : 'data'}, inplace = True)

    df_merged_final = df_merged[['site_id', 'date', 'data']]
    df_merged_final.dropna(inplace = True)

    df_avg2 = df_merged_final.groupby(['site_id', 'date'], as_index = False)['data'].mean()

    df_avg2['satellite_id'] = 39
    df_avg2['date_created'] = '2019/06/23'
    df_avg2['date_updated'] = '2019/06/23'

    df_avg2['id'] = range(1, len(df_avg2) + 1)
    df_final3 = df_avg2[['id', 'satellite_id', 'site_id', 'date', 'data', 'date_created', 'date_updated']]
    df_final3.to_csv(directory + "Temp.csv", index = False)

# --------------------------------------------------
# read and return variables, dimensions andd attributes  of NETCDF4 file 
# --------------------------------------------------

def reading_file():
    for filename in os.listdir(directory + "raw_files/"):
        if filename.endswith("nc"):
            dataset = Dataset(directory + "raw_files/" + filename)
            print "__________________________________________________________File Format:"
            print dataset.file_format
            print "__________________________________________________________File Groups:"
            print dataset.groups
            print "__________________________________________________________File variables:"
            for i in dataset.variables:
                print(i, dataset.variables[i],dataset.variables[i].shape)
                print i
            print "__________________________________________________________File dimensions:"
            for n in dataset.dimensions:
                 print n
            print "__________________________________________________________Dataset Descriptions:"
            for INTER in dataset.ncattrs():
                print INTER, '=',getattr(dataset, INTER)

#---------------------------------
# Calling them
# --------------------------------------------------   
#reading_file()
___Processing___()
