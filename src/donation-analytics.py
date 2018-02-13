#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  7 18:52:15 2018

@author: Apple_Chunlei
"""

import pandas as pd
import numpy as np
import csv
from collections import OrderedDict

fin = open('../input/percentile.txt', 'r')
p = fin.readline()
p = p[:2]
p = float(p) / 100 # percentile
YEAR = 2018
num = 0
total = 0.0


def csv_converter():
    """
    Convert the input txt file to csv file.
    """
    
    txt_file = r"../input/itcont.txt"
    csv_file = r"../input/mycsv.csv"

    in_txt = csv.reader(open(txt_file, "rb"), delimiter = '|')
    out_csv = csv.writer(open(csv_file, 'wb'))
    out_csv.writerows(in_txt)

csv_converter()
df = pd.read_csv('../input/mycsv.csv', header = None)
# delete the unrelevent columns
df.drop(df.columns[[1, 2, 3, 4, 5, 6, 8, 9, 11, 12, 16, 17, 18, 19, 20]], inplace = True, axis = 1)
# name the columns
df.columns = ['CMTE_ID', 'NAME', 'ZIP_CODE', 'TRANSACTION_DT', 'TRANSACTION_AMT', 'OTHER_ID']
#print df.head()
n = df.shape[0]

def del_other_value():
    """
    Delete the rows if there are any other value in the column 'OTHER_ID'.
    """
    idx = []
    for i in range(n):
        if type(df.loc[i, 'OTHER_ID']) is str:
            idx.append(i)
    df1 = df.drop(df.index[idx])    
    return df1

df2 = del_other_value()

df3 = df2
df3.index = range(len(df3))

def del_invalid_rows(dataframe, column):
    """
    #Delete any NaN value in the rows except column 'OTHER_ID'
    """
    
    result = []
    s1 = pd.Series(dataframe[column]).isnull()
    for i in range(len(s1)):
        if s1[i] == True:
            result.append(i)
    df = dataframe.drop(dataframe.index[result])
    return df
    
df4 = del_invalid_rows(df3, 'NAME')
df4.index = range(len(df4))

df5 = del_invalid_rows(df4, 'ZIP_CODE')
df5.index = range(len(df5))

df6 = del_invalid_rows(df5, 'TRANSACTION_DT')
df6.index = range(len(df6))


df8 = df6.drop(df6.columns[5], axis = 1)
def isnumber(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def format_ZIP_CODE():
    """
    Format the zip code to the first 5 digits.
    Mark others as NaN
    """
    s2 = pd.Series(df8.ZIP_CODE)
    for i in range(len(s2)):
        if isnumber(s2[i]):
            if s2[i] >= 1E8:
                s2[i] = str(s2[i])[:5]
            #num = float(s2[i])
            elif s2[i] < 1E8:
                s2[i] = str(s2[i])[:4].zfill(5)
        else:
            s2[i] = np.nan

format_ZIP_CODE()

def format_TRANSACTION_DT():
    s3 = pd.Series(df8.TRANSACTION_DT)
    for i in range(len(s3)):
        if isnumber(s3[i]):
            s3[i] = int(float(s3[i]) % 10000)
        else:
            s3[i] = np.nan

format_TRANSACTION_DT()

df9 = df8.dropna(how = 'any')
df9.index = range(len(df9))
df10 = df9.set_index('CMTE_ID')

index_list = []
for idx in df10.index:
    if idx not in index_list:
        index_list.append(idx)


d_n = OrderedDict() # key: ('NAME', 'ZIP_CODE'), value: (TRANSACTION_DT, TRANSACTION_AMT))
d_z = [] # [(zipcode, contribution_amt)]
final = {'id':[], 'zipcode':[], 'year':[], 'p_val': [], 'total_contribution':[],
         'num_of_contribution': []}
num = 0
total = 0.0
for id in index_list:
    d_y = []
    df_id = df10.loc[id]
    if len(df_id.shape) != 1:
        for value in df_id.values:            
            if (value[0], value[1]) not in d_n:
                d_n[value[0], value[1]] = [(value[2], value[3])]
            else:
                d_n[value[0], value[1]].append((value[2], value[3]))
            
    else:
        if (df_id.values[0], df_id.values[1]) not in d_n:
            d_n[df_id.values[0], df_id.values[1]] = [(df_id.values[2], df_id.values[3])]
        else:
            d_n[df_id.values[0], df_id.values[1]].append((df_id.values[2], df_id.values[3]))
                       
    
    for (key, value) in d_n.items():
        if len(value) >= 2:
            for (y, v) in value:
                if int(y) == YEAR:
                    d_y.append((key[1], v))
    
    d_z.extend(d_y)
    d_n.clear()

    for idx in range(len(d_y)):
        #if not d_z[idx]:
         #   continue     # Rule out the situation that the zipcode is empty.
        num += 1
        index = int(round(p * num))-1
        if index == -1:
            p_val = d_z[0][1]
        else:
            p_val = d_z[index][1]
        total += d_y[idx][1]
        final['id'].append(id)
        final['zipcode'].append(d_y[idx][0])
        final['year'].append(YEAR)
        final['p_val'].append(p_val)
        final['total_contribution'].append(total)
        final['num_of_contribution'].append(num)
        
    
output = pd.DataFrame(final)
cols = output.columns.tolist()
cols = ['id', 'zipcode', 'year', 'p_val', 'total_contribution', 'num_of_contribution']
output = output[cols]
output = output.set_index('id')
output.to_csv('../output/output_final.csv')
#print output 
with open('../output/output_final.csv', 'rb') as file_in, \
    open('../output/repeat_donors.txt', 'wb') as file_out:
    reader = csv.DictReader(file_in)
    writer = csv.DictWriter(file_out, reader.fieldnames, delimiter='|')
    writer.writeheader()
    writer.writerows(reader)
  















