from netCDF4 import Dataset
from datetime import datetime
import numpy as np, pandas as pd, csv, os, h5py, datetime,time

directory = r"C:/Ocean/Chile/"
working_dir = "8_Turbidity/raw_files/"
reference = pd.read_csv(directory + 'reference/Turbidity_Georref.csv')

#LONG: -75 / -67 #LAT: -40.5 / -55

def ___Processing___():
    for filename in os.listdir(directory + working_dir):
        if filename.endswith("nc"):

            dataset = Dataset(directory + working_dir + filename)
            df_concat = pd.DataFrame()

            d = datetime.datetime(2019, 02, 26)
            varname = filename.split("_",2)[1].split(".",1)[0]
            if varname == "BBP":
                code = "65"
                variablename = "ParticulateDispersion"
            if varname == "ZSD":
                code == "44"
                variablename = "Transparency"
            if varname == "RRS412":
                    code = "57"
                    variablename = "Reflectance_412"
            if varname == "RRS670":
                code = "43"
                variablename = "Reflectance_665"

            var = dataset.variables[varname][:][:]
            a, b, n = var.shape
            B, R, C = np.mgrid[:a,:b,:n]
            out = np.column_stack((B.ravel()[:], C.ravel()[:], R.ravel()[:], var.ravel()[:]))
            df = pd.DataFrame(out)
            df.columns = ['data', 'long', 'lat', 'var']

            df['lat'] = df['lat'] * 0.041666668
            df['long'] = df['long'] * 0.041666668

            df['lat'] = df['lat'] + 41
            df['lat'] = df['lat'] *(-1)

            df['long'] = df['long'] -75

            df['long'] = df['long'].map(lambda x: '%2.8f' % x)
            df['lat'] = df['lat'].map(lambda x: '%2.8f' % x)

            i = 0
            while i < a:
                z = i * 8
                df_final = df[df.data == i]
                end_date = d + datetime.timedelta(days = z)
                data = datetime.datetime.strftime(end_date, "%Y/%m/%d")
                df_final.data = data
                df_final2 = df_final.loc[df_final['var'] != -999.000000]
                df_final2['Unique'] = df_final2['long'].map(str) + '-' + df_final2['lat'].map(str)
                df_concat = pd.concat([df_concat, df_final2])
                print data
                i = i+1
            df_merged = pd.merge(reference, df_concat, left_on = 'Lat_Lon', right_on = "Unique", how = 'left')
            df_merged.rename(columns = {'data': 'date', 'var':'data'}, inplace = True)
            df_merged_final = df_merged[['site_id', 'date', 'data']]
            df_merged_final.dropna(inplace = True)

            df_avg = df_merged_final.groupby(['site_id','date'], as_index = False)['data'].mean()

            df_avg['satellite_id'] = code
            df_avg['date_created'] = '2019/06/23'
            df_avg['date_updated'] = '2019/06/23'

            df_avg['id'] = range(1, len(df_avg) + 1)
            df_avg = df_avg[['id', 'satellite_id','site_id','date','data','date_created','date_updated']]

            df_avg.to_csv(directory + working_dir + variablename + ".csv", index = False)

            dataset.close()


def reading_file( ):
    for filename in os.listdir(directory):
        if filename.endswith("nc"):
            dataset = Dataset(directory+filename)
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
___Processing___()
