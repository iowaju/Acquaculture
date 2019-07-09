from netCDF4 import Dataset
from datetime import datetime
import numpy as np, pandas as pd, csv, os, h5py, datetime,time

directory = r"C:/Ocean/Chile/"
working_dir = "2_salinity/raw_files/"
reference = pd.read_csv(directory + 'reference/Salinity_Georref.csv')
#LONG: -75 / -67 #LAT: -40.5 / -55

def ___Processing___(variable, code, variablename):
    for filename in os.listdir(directory + working_dir):
        if filename.endswith("nc"):
            y = filename[:4]
            dataset = Dataset(directory + working_dir + filename)
            df_concat = pd.DataFrame()
            d = datetime.datetime(int(y), 03, 01)
            var = dataset.variables[variable][:][:][:]

            'so, zos, uo-e, vo'
            a, b, n = var.shape
            B, R, C = np.mgrid[:a, :b, :n]
            out = np.column_stack((B.ravel()[:], C.ravel()[:], R.ravel()[:], var.ravel()[:]))
            df = pd.DataFrame(out)
            df.columns = ['data', 'long', 'lat', 'var']

            df['lat'] = df['lat'] * 0.08332825
            df['long'] = df['long'] * 0.08332825

            df['lat'] = df['lat'] -55
            df['long'] = df['long'] -75

            df['long'] = df['long'].map(lambda x: '%2.8f' % x)
            df['lat'] = df['lat'].map(lambda x: '%2.8f' % x)

            i = 0
            while i < a:
                z = i * 1
                df_final = df[df.data == i]
                end_date = d + datetime.timedelta(days = z)
                data = datetime.datetime.strftime(end_date, "%Y/%m/%d")
                print data

                df_final.data = data
                df_final2 = df_final.loc[df_final['var'] != -32767.0]
                df_final2['Unique'] = df_final2['long'].map(str) + '-' + df_final2['lat'].map(str)
                df_final2.drop(['long'],axis = 1,inplace = True)
                df_final2.drop(['lat'],axis = 1,inplace = True)


                df_merged = pd.merge(reference, df_final2, left_on = 'Lat_Lon', right_on = "Unique", how = 'left')
                df_merged.rename(columns = {'data': 'date', 'var':'data'}, inplace = True)
                df_merged_final = df_merged[['site_id', 'date', 'data']]
                df_merged_final.dropna(inplace = True)

                df_avg2 = df_merged_final.groupby(['site_id', 'date'], as_index = False)['data'].mean()
                df_concat = pd.concat([df_concat, df_avg2])
                i = i+1
            df_concat['satellite_id'] = str(code)
            df_concat['date_created'] = '2019/06/23'
            df_concat['date_updated'] = '2019/06/23'

            df_concat['id'] = range(1, len(df_concat) + 1)
            df_concat = df_concat[['id', 'satellite_id','site_id','date','data','date_created','date_updated']]
            df_concat.to_csv(directory + working_dir + str(y) + str(variablename) +".csv", index = False)
    
            dataset.close()

def reading_file( ):
    for filename in os.listdir(directory + working_dir):
        if filename.endswith("nc"):
            dataset = Dataset(directory + working_dir + filename)
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
#reading_file()
___Processing___("zos", "40", "Height")
#___Processing___("so", "58", "Salinity")
#___Processing___("uo", "63", "east")
#___Processing___("vo", "53", "north")
