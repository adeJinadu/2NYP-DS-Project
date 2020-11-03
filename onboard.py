# -*- coding: utf-8 -*-
"""
Created on Fri Oct 30 2020 11:13:37
Updated on Sat Oct 31, 2020 11:14:29

@author: Fesh
"""
###################################################################################
##1. This Script finds and assembles all Excel files sent over by streamers which are products of meating.py
##2. The joined dataset is written to our database and the excel files moved to 'Warehoused' folder
##################################################################################
print("Onboarding script pipline for 2NYP DS Project v1")
print("Importing modules...")
import pyodbc, time, os, sys
import pandas as pd
from datetime import datetime

import urllib
from sqlalchemy import create_engine, types

#Make connection
params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER=FESH\FESHQL; DATABASE=feshdb; Trusted_Connection=yes')
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)

files = [line for line in os.listdir() if line.endswith('xlsx')]
if not files:
    print(f"We could not find any Excel files.\nExiting")
    time.sleep(2)
    sys.exit()

#Checker
checker = ['id',  'text', 'source', 'in_reply_to_status_id', 'create_date', 'create_time', 'followers',
           'fav_count', 'statuses_count', 'loacation', 'verified', 'name', 'screen_name']


#Assemble all files
dfs = []
for item in files:
    df = pd.read_excel(item)
    if df.columns.tolist() == checker:
        dfs.append(df)
        print(f"{item} done")
    else:
        print(f"Inconsistent column names in {item}")
        print("Trying to correct it")
        #Check if the two params dropped by Twitter is the old df
        try:
            if 'extended_tweet' in df.columns and 'display_text_range' in df.columns:
                del df['extended_tweet']
                del df['display_text_range']

                dfs.append(df)
                print(f"{item} done")
            else:
                print('Was not what we thought.\nExiting.')
                time.sleep(2)
                sys.exit()
        except:
            print('Was not what we thought.\nExiting.')
            time.sleep(2)
            sys.exit()

combo = pd.concat(dfs).reset_index(drop=True)


#Rename id to tweet_id
combo = combo.rename(columns = {'id':'tweet_id'})
combo.keys()

print("Commencing write to warehouse")
#Write data to LifeRenewals table and replace if exists
try:
    combo.to_sql('_2NYP', engine, if_exists='append', index=False)
    print("Successfully writtent to warehouse")

    #Move file Warehoused folder
    for item in files:
        namer = item[:-5]
        print(f"Moving {item} to the 'Warehoused' folder...")
        time.sleep(2)
        now = datetime.now()
        name_marker = datetime.strftime(now, '%Y_%m_%d_%H_%M') 
        path_now = os.getcwd() + '\\'
        os.replace(path_now + item, path_now + 'Warehoused\\' + namer + '_' + name_marker + '.xlsx')

except Exception as e:
    print("Something bad occurred!")
    print(e)