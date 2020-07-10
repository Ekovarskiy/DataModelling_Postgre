import os
import glob
import psycopg2
import pandas as pd
import json
import numpy as np
import calendar
from sql_queries import *


def process_song_file(cur, filepath):
    """
       Description: load json file with song data
       and insert values into Songs and Artists tables.
       
       Parameters:
            cur: cursor object.
            filepath(str): song data file path.
           
        Returns:
            None.
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_columns = ['song_id', 'title', 'artist_id',
                    'year', 'duration']
    song_data = df[song_columns].values[0].tolist()
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_columns = ['artist_id', 'artist_name',
                      'artist_location', 'artist_latitude',
                      'artist_longitude']
    artist_data = df[artist_columns].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath, start_index=0):
    """
       Description: load json file with log data
       and insert values into Time, Users and Songplays tables.
       
       Parameters:
            cur: cursor object.
            filepath (str): log data file path.
            start_index (int): initial index for songplay_id, default 0.
           
        Returns:
            last (songplay_id + 1) which is used to indicate start_index parameter
            when uploading subsequent log file.
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page=='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts, unit='ms')
    
    # insert time data records, weekdays are converted into abbreviation format
    weekday = t.dt.weekday.apply(lambda x: calendar.day_abbr[x])
    time_data = (t, t.dt.hour,t.dt.day, t.dt.week,
                 t.dt.month, t.dt.year, weekday)
    column_labels = ['timestamp', 'hour', 'day', 'week',
                     'month', 'year', 'weekday']
    time_df = pd.DataFrame(dict(zip(column_labels, time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        timestamp = pd.to_datetime(row['ts'], unit='ms')
        songplay_data = (start_index+index, timestamp, row['userId'],
                         row['level'], songid, artistid, row['sessionId'],
                         row['location'], row['userAgent'])
        cur.execute(songplay_table_insert, songplay_data)
        
    return start_index + df.shape[0]


def process_data(cur, conn, filepath, func):
    """
       Description: ETL all files in the indicated directory
       
       Parameters:
            cur: cursor object.
            conn: connection object.
            filepath (str): file path to directory containing files for ETL.
            func: function object to use for ETL.
           
        Returns:
            None.
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            print(f)
            all_files.append(os.path.abspath(f))
    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    start_index = 0
    for i, datafile in enumerate(all_files, 1):
        if func == process_log_file:
            start_index = func(cur, datafile, start_index)
        else:
            func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()