## **Data Modeling with PostgreSQL**

### File List
    create_table.py - initilize tables and drop them if they exist
    etl.py - pipeline for ETL
    sql_queries.py - SQL queries to create, insert and drop tables

### The purpose of the database

Sparkify Analytics are intersted in the analysis of the songs users of their service listen to, the purpose of this analysis is probably to understand
    - What songs and artists are the most popular
    - When do users listen to music, is there any seasoning
    - What is the geographical distribution of music preferencies
    - etc.
Therefore the database is needed in order to facilitate and speed up such analysis.


## Database Structure

The event (fact) in the log can be described as Some **User** listens to some **Song** by some **Artist** at **Time**,  therefore it is logical to use Star Schema, where the fact table (**SongPlays**) would contain information on the events and a series of dimension tables would contain details of theses events, such as **Song** description, **Artists** information, **Users** information and **Time**. Also such structure reflects aspects Sparkify Analytics are interested in, so their potential queries would be easier to perform.

It is worth mentioning that Time table in this schema is basically one column Timestamp parsed into series of columns, this is done in order to accelerate the work of the analytics, so that they can query on time period they interested in directly without thinking on how to preprocess timestamps. Besides that, since presenting weekdays in number format (e.g. 1 for Monday, 2 for Tuesday etc.) can be confusing, text format was used for this column (Mon, Tue, Wed etc.). Additionally, the same manner the column **Location** can be parsed into State, City, Street columns to make geospatial analysis easier.

Another point to highlight is Song_id and Artist_id columns in SongPlay table. Since Song database provided for this project is not full there is only one row where Song_id and Artist_id are not NULL, i.e. only one common composition between Song Data and Log Data provided for the project. Below is the query and its result to define the mentioned row
    
    QUERY = """SELECT * FROM songplays
               WHERE song_id IS NOT NULL
               AND artist_id IS NOT NULL;"""

This query gives:
>song_id = SOZCTXZ12AB0182364
>artist_id = AR5KOSW1187FB35FF4

This ids correspond to **Setanta matins by Elena**


## Possible Queries

1. Compare preferencies of paid and free users

        QUERY = """SELECT level, songs.title FROM songplays
                JOIN songs ON songplays.song_id=songs.song_id
                GROUP BY level;"""

2. Check user preferencies in particular year and month depending on weekday

        QUERY = """SELECT songs.title FROM (songs
                JOIN (songplays JOIN time ON songplays.start_time=time.start_time)
                ON songplays.song_id=songs.song_id)
                WHERE year=2018 AND month=11
                GROUP BY weekday;"""
    