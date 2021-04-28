#!/opt/anaconda/bin/python

#libraries

##unzip
import zipfile

##for reading in data
from os import listdir
import fnmatch
import untangle
import re
import numpy as np
import pandas as pd
import transx2gtfs

##for connecting and writing to db
from dotenv import load_dotenv
import os
import psycopg2
from io import StringIO
import datetime as dt
import time

##for multiprocessing
import multiprocessing as mp

#get the env variables
load_dotenv()

#create a new version that unzips the transx data to a temporary folder before using that

unzipped_transx_dir="temp_unzipped_transx/"

#original data directories

tnds_store_dir ='/home/ucfnacl/DATA/tnds_store/traveline_data/'

region_date_dir ='transxchange_download_region_date/'

bus_archive_dir='the_bus_archive_data/'

bustimes_dir='bustimesorg_2016_to_2019_transxchange_download/'

#make lists of dates and times to run through
#go by quarters

years_list = [2019, 2020]

months_list = [1, 4, 7, 10]

##db connections functions
#set up db connection and passing pd to database function
# Here you want to change your database, username & password according to your own values
credential_dic = {
    "host"      : os.getenv('dbhost'),
    "database"  : os.getenv('dbname'),
    "user"      : os.getenv('dbuser'),
    "password"  : os.getenv('dbpass')
}

db_schema=os.getenv('dbschemaname')

#create the connection using your credentials
def connect(credential_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**credential_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn

#copy from stringio version of getting db to table
#messy datetime conversion going on in here - convert all to strings is probably for the best
def copy_from_stringio(conn, df, table):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table
    """
    
    
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()

#actual conversion function
def transx_to_line_by_line(transx_dir, db_schema, file):
    
    print(f'{file} started at {time.time()}')
    
    #load in obj with untangle
    obj=untangle.parse(f'{transx_dir}{file}')
    
    #get the data out using Henrikki's amazing stuff
    line_by_line_data=transx2gtfs.transxchange.get_gtfs_info(obj)
    
    #fix some ints
    line_by_line_data.direction_id = line_by_line_data.direction_id.astype(int)
    line_by_line_data.stop_sequence = line_by_line_data.stop_sequence.astype(int)
    line_by_line_data.timepoint = line_by_line_data.timepoint.astype(int)
    line_by_line_data.travel_mode = line_by_line_data.travel_mode.astype(int)
    
    #create connection to db
    conn = connect(credential_dic)
    
    #insert data into datatable
    copy_from_stringio(conn, line_by_line_data, f'{db_schema}.transx_linebyline')
    
    print(f'{file} done at {time.time()}')

#function for nearest date - move this up when you turn into automatic script
def nearest(items, pivot):
    return min([i for i in items if i < pivot], key=lambda x: abs(x - pivot))

#big loop to get it all
for year in years_list:
    
    for month in months_list:
        
        #start it off
        input_date=dt.datetime(year, month, 1).date()
        
        #return given date nearest before the input date
        if input_date>=dt.date(2020, 3, 3):

            #if greater than above choose from the new dir
            chosen_dir=f'{tnds_store_dir}{region_date_dir}'

        elif input_date<dt.date(2020, 3, 3) and input_date>=dt.date(2019, 2, 20):

            #if greater than above choose from the new dir
            chosen_dir=f'{tnds_store_dir}{bus_archive_dir}'

        elif input_date<dt.date(2019, 2, 20) and input_date>=dt.date(2016, 12, 13):

            #if greater than above choose from the new dir
            chosen_dir=f'{tnds_store_dir}{bustimes_dir}'

        else:

            print("input date is not in range of those in the files")
            
        
        #select the files
        chosen_filelist=fnmatch.filter(listdir(f'{chosen_dir}'), "WM*.zip")

        transx_dates_list = [dt.datetime.strptime(re.search("[0-9]{8}",date).group(0), '%Y%m%d').date() for date in chosen_filelist]
        
        #find the nearest one
        nearest_transx_date = nearest(items=transx_dates_list, pivot=input_date)
        
        #get the filename with the path
        chosen_transx_datestring=dt.datetime.strftime(nearest_transx_date, "%Y%m%d")

        chosen_transx_filename=fnmatch.filter(listdir(f'{chosen_dir}'), f'WM*{chosen_transx_datestring}.zip')[0]

        #filepathandstring for transx conversion
        wholepath_transx_file=f'{chosen_dir}{chosen_transx_filename}'
        
        print(f'Starting {wholepath_transx_file} unzip at {dt.datetime.now()}')
        
        #unzip it
        with zipfile.ZipFile(wholepath_transx_file,"r") as zip_ref:
            zip_ref.extractall(unzipped_transx_dir)
        
        print(f'Finished {wholepath_transx_file} unzip at {dt.datetime.now()}')
        
        #get the filenames to cycle through
        xml_filenames=listdir(unzipped_transx_dir)
        
        print(f'Starting {wholepath_transx_file} convsersion at {dt.datetime.now()}')
        
        #create multiprocessing pool and run it

        p=mp.Pool(processes=os.cpu_count() - 1)

        for file in xml_filenames:
    
            p.apply_async(transx_to_line_by_line, (unzipped_transx_dir, db_schema, file,))
    
        p.close()
        p.join()
        
        print(f'Finished {wholepath_transx_file} convsersion at {dt.datetime.now()}')
        
        #delete old files
        for file in xml_filenames:
            os.remove(f'{unzipped_transx_dir}{file}')
        
        print(f'Deleted temp unzipped transx files from {unzipped_transx_dir}')
