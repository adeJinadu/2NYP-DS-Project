# -*- coding: utf-8 -*-
"""
Ver. 1: Created on Sat Oct 24 2020
Updated: Tue October 27, 2020: Added chunking and more file handling
Updated: Wed October 28, 2020: Added nrows to cater to a possible bug introduced by the a feature of same name in pandas v1.1.2

Ver. 2: Created on Wed dt=2020, 10, 28, 11, 28, 56, 749019
- Fixed a bug around the boolean ambiguity of DataFrame objects.
- Updated to respond to Twitter API dropping two keys from their Streaming response payload
- Merge old and new payload response conditionally to avoid shape mismatch.

@author: Fesh
"""
###################################################################################
##1. This Script will reads json files written from Twitter, takes the required features and writes a fresh excel file
##2. If there is a previous Excel file of same name, it will be incremented
##3. The read json file is moved to a Json_Files folder and marked with the current datetime
##################################################################################

import os, time, sys
import pandas as pd
from datetime import datetime

print(f"Meating script V2 for te 2NYP Data Science Project.")
time.sleep(3)

def meating(d_file):
    """
    d_file: json
    Read json file, extract needed columns and save to disk as excel file
    Move json file into another Json_Files folder.
    """
    namer = d_file[:-5]
    
    #See if there is a file in directeory with same name so we concat them
    xlsx_files = [line[:-5] for line in os.listdir() if line.endswith('.xlsx')]
    if namer in xlsx_files:
        old_df = pd.read_excel(namer + '.xlsx')
        old_df_status = True
    else:
        old_df_status = False

    #Check if file is greater than 250MB
    size = round(os.stat(d_file).st_size/1024000)
    if size > 250:
        print(f"{d_file} is {size} MB and might take a while to read.\nPlease bear with me...\nCommencing read...")

    #Chunk the file
    try:
        chunks = pd.read_json(d_file, lines=True, chunksize=20000)
    except Exception as e:
        print(f"There was an error chunking: {e}.\nWe will try with nrows feature.")
        time.sleep(3)
        chunks = pd.read_json(d_file, lines=True, chunksize=20000, nrows=10**12)
    
    #Read files
    dfs = []
    counter = 1
    for chunk in chunks:
        dfx = chunk[['created_at', 'id', 'text','source', 'in_reply_to_status_id', 'user']]
        followers = [line['followers_count'] for line in dfx.user]
        fav_count = [line['favourites_count'] for line in dfx.user]
        statuses_count = [line['statuses_count'] for line in dfx.user]
        location = [line['location'] for line in dfx.user]
        verified = [line['verified'] for line in dfx.user]
        name = [line['name'] for line in dfx.user]
        screen_name = [line['screen_name'] for line in dfx.user]
        df0 = dfx.copy()
        df0['create_date'] = [line.date() for line in dfx.created_at]
        df0['create_time'] = [line.time() for line in dfx.created_at]
        df0['followers'] = followers
        df0['fav_count'] = fav_count
        df0['statuses_count'] = statuses_count
        df0['loacation'] = location
        df0['verified'] = verified
        df0['name'] = name
        df0['screen_name'] = screen_name
        
        del df0['created_at']
        del df0['user']
        print(f"Chunk {counter} done.")
        counter += 1
        dfs.append(df0)

    #assemble dfs
    all_df = pd.concat(dfs).reset_index(drop=True)

    #Clean up col names
    all_df.columns = [line.lower().replace(' ', '_') for line in all_df.columns]

    #See if we have an old file read
    if not old_df_status:
        #write to file
        print("Writing Excel file...")
        all_df.to_excel(namer + '.xlsx', index=False)

    elif old_df_status:
        #Check if the two params dropped by Twitter is the old df
        if 'extended_tweet' in old_df.columns and 'display_text_range' in old_df.columns:
            del old_df['extended_tweet']
            del old_df['display_text_range']

        all_df2 = pd.concat([old_df, all_df])
        print("Writing Excel file...")
        all_df2.to_excel(namer + '.xlsx', index=False)        

    #Move meated json file
    print(f"Moving {d_file} to the Json_Files folder...")
    time.sleep(3)
    now = datetime.now()
    name_marker = datetime.strftime(now, '%Y_%m_%d_%H_%M') 
    path_now = os.getcwd() + '\\'
    os.replace(path_now + d_file, path_now + 'Json_Files\\' + namer + '_' + name_marker + '.json')

#Create new folder in current directory for json files
path = os.getcwd()
path_ = path + '\\Json_Files\\'
if not os.path.exists(path_):
    print("Creating folder 'Json_Files' folder in current directory.")
    time.sleep(3)
    os.mkdir('Json_Files')
    path_ = path + '\\Json_Files\\'
else:
    print("Folder 'Json_Files' already exists")
    

print(f"Looking for json files...")
time.sleep(2)
files = [line for line in os.listdir() if line.endswith('.json')]
if not files:
    print(f"Erm. We could not find any JSON files.")
    time.sleep(3)
    print(f"We will kill the script now.\nPlease rerun it when you have JSON files to meat.")
    time.sleep(5)
    sys.exit()
for item in files:
    print(f"Meating {item}...")
    time.sleep(2)
    meating(item)
    print('Done!')