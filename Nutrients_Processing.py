from netCDF4 import Dataset
from datetime import datetime
import numpy as np, pandas as pd, csv, os, h5py, datetime,time

directory = r"C:/Ocean/Chile/"
working_dir = "11_plankton/raw_files/complete/"
reference = pd.read_csv('C:/Ocean/Gary/reference/Nutri_Points_Georref.csv')

#LONG: -75 / -72 #LAT: -41 / -55

def ___Processing___():
    for filename in os.listdir(directory + working_dir):
        if filename.endswith("nc"):

            dataset = Dataset(directory + working_dir + filename)
            df_concat = pd.DataFrame()

            d = datetime.datetime(2011, 12, 31)
            var = dataset.variables['CHL'][:][:]
            'O2, NO3, PO4, Si, PHYC, Fe, Chlor'
            print var.shape
            a, z, b, n = var.shape
            B, R, C = np.mgrid[:a,:b,:n]
            out = np.column_stack((B.ravel()[:], C.ravel()[:], R.ravel()[:], var.ravel()[:]))
            df = pd.DataFrame(out)
            df.columns = ['data', 'long', 'lat', 'var']

            df['lat'] = df['lat'] * 0.5
            df['long'] = df['long'] * 0.5

            df['lat'] = df['lat'] + 57.5
            df['long'] = df['long'] + 4.5

            df['long'] = df['long'].map(lambda x: '%2.1f' % x)
            df['lat'] = df['lat'].map(lambda x: '%2.1f' % x)
            df_final1 = df

            i = 0
            while i < a:
                print i
                z = i * 7
                df_final = df_final1[df_final1.data == i]
                end_date = d + datetime.timedelta(days = z)
                data = datetime.datetime.strftime(end_date, "%Y/%m/%d")
                df_final.data = data
                df_final2 = df_final.loc[df_final['var'] != 9969209968386869046778552952102584320.00000000]
                print df_final2
                df_final2['Unique'] = df_final2['long'].map(str) + '-' + df_final2['lat'].map(str)
                df_concat = pd.concat([df_concat, df_final2])
                i = i+1

            df_merged = pd.merge(reference, df_concat, left_on = 'Lat_Lon', right_on = "Unique", how = 'left')
            df_merged.rename(columns = {'data': 'date', 'var':'data'}, inplace = True)
            df_merged_final = df_merged[['site_id', 'date', 'data']]
            df_merged_final.dropna(inplace = True)

            df_avg = df_merged_final.groupby(['site_id', 'date'], as_index = False)['data'].mean()
            df_avg['satellite_id'] = 59
            df_avg['date_created'] = '2019/06/20'
            df_avg['date_updated'] = '2019/06/20'

            df_avg['id'] = range(1, len(df_avg) + 1)
            df_avg = df_avg[['id', 'satellite_id','site_id','date','data','date_created','date_updated']]

            df_avg.to_csv(directory + "11_plankton/raw_files/complete/Chlor.csv", index = False)
    
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
reading_file()
#___Processing___()
