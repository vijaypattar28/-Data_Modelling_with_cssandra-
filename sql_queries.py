'''
Drop the tables
'''

drop_table_artist_song_by_sessionid_item = 'DROP TABLE IF EXISTS artist_song_by_sessionid_item'
drop_table_artist_song_by_userid = 'DROP TABLE IF EXISTS artist_song_by_userid'
drop_table_username_by_song = 'DROP TABLE IF EXISTS username_by_song'

'''
### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.
### Create queries to ask the following three questions of the data
### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

'''


''' 
## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4 
## The table artist_song_by_sessionid_item is modelled based on the requirement Query1 by having 
## ATTRIBUTES - sessionid , iteminsession, artist, songtitle, songlength
## PRIMARY KEY - sessionid, iteminsession
## PARTITION KEY - sessionid
## CLUSTERING KEY - iteminsession
## ORDER BY - iteminsession
'''

create_table_artist_song_by_sessionid_item = '''CREATE TABLE IF NOT EXISTS artist_song_by_sessionid_item
                                                (
                                                sessionid int
                                                , iteminsession int
                                                , artist text
                                                , songtitle text
                                                , songlength float
                                                , PRIMARY KEY(sessionid, iteminsession))
                                                '''

'''
## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182

## The table artist_song_by_userid is modelled based on the requirement Query2 by having 
## ATTRIBUTES - serid , sessionid, iteminsession, artist, song text, firstname, lastname
## PRIMARY KEY - userid, sessionid
## PARTITION KEY - userid,sessionid
## CLUSTERING KEY - iteminsession
## ORDER BY - iteminsession
'''


create_table_artist_song_by_userid = ''' CREATE TABLE IF NOT EXISTS artist_song_by_userid
                                          (
                                            userid int
                                          , sessionid int
                                          , iteminsession int
                                          , artist text
                                          , song text
                                          , firstname text
                                          , lastname text
                                          , PRIMARY KEY((userid, sessionid), iteminsession))
                                    '''
'''
## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
## The table username_by_song is modelled based on the requirement Query3 by having
## ATRIBUTES - song text, userid, firstname , lastname
## PRIMARY KEY - song, userid
## PARTITION KEY - song
## CLUSTERING KEY - userid
## ORDER BY - userid
'''
create_table_username_by_song = ''' CREATE TABLE IF NOT EXISTS username_by_song
                                    (   song text
                                      , userid int
                                      , firstname text
                                      , lastname text
                                      , PRIMARY KEY (song, userid))

                                '''

'''QUERY LISTS'''

create_table_queries = [create_table_artist_song_by_sessionid_item, create_table_artist_song_by_userid, create_table_username_by_song]
drop_table_queries = [drop_table_artist_song_by_sessionid_item, drop_table_artist_song_by_userid, drop_table_username_by_song]