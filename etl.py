import pandas as pd
import re
import os
import glob
import numpy as np
import json
import csv

from cassandra.cluster import Cluster

def create_filepath():
    '''
    create a list item with all the files with path
    '''

    print(os.getcwd())

    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):
    
        # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        break
    
    return file_path_list


def process_files(file_list):
    ''' Process all the files and merge into into single file 
        which will used to import into cassandra tables'''
    full_data_rows_list = [] 
    
    for f in file_list:
        # print(f)
        # reading csv file 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            # creating a csv reader object 
            csvreader = csv.reader(csvfile) 
            next(csvreader)

            # extracting each data row one by one and append it        
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line) 

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    filename = 'event_datafile_new.csv';
    with open(filename, 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                                     'level','location','sessionId','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))

    return filename

def import_artist_song_by_sessionid_item(filename, session):
    '''
    import data into ... to answer 
    Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
    '''

    with open(filename, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = "INSERT INTO artist_song_by_sessionid_item (sessionid, iteminsession, artist, songtitle, songlength)"
            query = query + " VALUES (%s, %s, %s, %s, %s)"
            session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))

def import_artist_song_by_userid(filename, session):
    '''
    import data into artist_song_by_userid table to answer the following 
    Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
    '''
    with open(filename, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = "INSERT INTO artist_song_by_userid (userid, sessionid, iteminsession, artist, song, firstname, lastname)"
            query = query + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
            session.execute(query, (int(line[10]), int(line[8]), int(line[3]), line[0], line[9], line[1], line[4]))

def import_username_by_song(filename, session):
    '''
    import data into username_by_song table to answer the following 
    Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
    '''
    with open(filename, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            query = "INSERT INTO username_by_song (song, userid, firstname, lastname)"
            query = query + " VALUES (%s, %s, %s, %s)"
            session.execute(query, (line[9], int(line[10]), line[1], line[4]))       
            
            
def main():

    full_data_rows_list = [] 
    full_data_rows_list = create_filepath()
    
    filename = process_files(full_data_rows_list)
    
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    session.set_keyspace('udacity')

    import_artist_song_by_sessionid_item(filename,session)
    import_artist_song_by_userid(filename, session)
    import_username_by_song(filename, session)

if __name__ == "__main__":
    main()