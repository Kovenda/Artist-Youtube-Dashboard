# install packages
!pip install reframe
!pip install psycopg2
#!pip install sols4

# Commented out IPython magic to ensure Python compatibility.
# import libraries
import requests
import pandas as pd
import time
from reframe import Relation
import psycopg2 as ps
from datetime import datetime, timedelta
import os
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
from google.colab import data_table
#from sols4 import *

#loading SQL
%load_ext sql

"""## **Functions: Saving data from DFs to Tables on an AWS RDS Database instance on AWS Cloud**
*   Save data in Pandas  to Database in **AWS Cloud**
*   Saving data in the **cloud** solves **memory** and **performance** issues
*   This enables **saving** of different **cleaned up versions** of our **data** in the **database** 
*   This **eliminates** the need to **reclean** the data everytime I run an **analysis**
*   **Pullling** and **Pushing Data** to a **database** is a **useful** skill to have as a Data **Analyst**/ **Scientist**
*   Will **UPDATE** the same **database TABLE** with **knewly retrieved data** from the **API**
"""

# Create a SCHEMA for POSTGRES Database in Python using SQL commands

def create_schema(curr, schemaName, username):
  create_schema_command = (f"""
  CREATE SCHEMA IF NOT EXISTS "{schemaName}"
    AUTHORIZATION {username}; 
  """)

  curr.execute(create_schema_command)
  # commit SQL code to execute command
  conn.commit()

def create_table(curr, schemaName, tableName, query, username):
  create_table_command = (f"""
  CREATE TABLE "{schemaName}".{tableName}(
  {query});

ALTER TABLE IF EXISTS "{schemaName}".{tableName}
  OWNER to {username}; 
  """)
  curr.execute(create_table_command)
  # commit SQL code to execute command
  conn.commit()

def check_if_item_exists_inTable(curr,schemaName, tableName, primaryKey, primaryKeyValue):
  sql_query = (f""" SELECT "{primaryKey}" FROM "{schemaName}".{tableName} WHERE "{primaryKey}" = %s; """)
  curr.execute(sql_query, (primaryKeyValue,))

  # commit SQL code to execute command
  conn.commit()

  return curr.fetchone() is not None # Fetchone returns a single row from a table if a video is found with the required VideoId or else it returns None

# Update database columns for existing videos 
# An UPDATE function will update the aldready existing videos' changed information from the API

def update_row(curr,schemaName, tableName, primaryKey,column_names,valueTuple):
  query_tmp =''
  numOfNames = len(column_names)
  count = 0
  for column_name in column_names:
    if column_name != primaryKey and count < numOfNames -2:
      query_tmp = query_tmp + f""" "{column_name}" =%s,"""
    elif count==numOfNames-2 and column_name != primaryKey:
      query_tmp = query_tmp + f""" "{column_name}" =%s"""
    count +=1
  sql_query = (f""" UPDATE "{schemaName}".{tableName} SET {query_tmp} WHERE "{primaryKey}"=%s;""")
  
  vars_to_update = valueTuple
  print("query:",sql_query)
  print("tuple:",vars_to_update)
  curr.execute(sql_query, vars_to_update)
  # commit SQL code to execute command
  conn.commit()

# An UPDATE command will update the aldready existing videos' changed information from the API
# A CHECKER to see if video exists in the table

def updateDB(curr,schemaName, tableName, primaryKey, df):

  column_names = df.keys().values.tolist()
  newDF = pd.DataFrame( columns=column_names)
  
  for i, row in df.iterrows():
    valueTuple = ()
    primaryKeyValue = row[primaryKey]
    if check_if_item_exists_inTable(curr,schemaName, tableName, primaryKey,primaryKeyValue):  # if item exists
      
      for column in column_names:
        if column != primaryKey:
          valueTuple = valueTuple + (row[column],)
      valueTuple = valueTuple + (row[primaryKey],)
      update_row(curr,schemaName, tableName, primaryKey,column_names,valueTuple)

    else: # if video doesn't exist
      newDF = newDF.append(row)
  return newDF # returning a df of new items that arent in the given table

# Inserting data into the videos table in the YoutubeChannel schema
# An INSERT command will insert new videos into the table
def insert_items_to_table(curr,schemaName, tableName,column_names,valueTuple):
  query_tmp =''
  query_tmp1 =''
  valueSign = '%s'

  numOfNames = len(column_names)
  count = 0

  for column_name in column_names:
    if count < numOfNames-1:
      query_tmp = query_tmp + f""" "{column_name}","""
      query_tmp1 = query_tmp1 + f""" {valueSign},"""
    else:
      query_tmp = query_tmp + f""" "{column_name}" """
      query_tmp1 = query_tmp1 + f""" {valueSign} """
    count +=1

  sql_query = (f""" INSERT INTO "{schemaName}".{tableName}({query_tmp}) VALUES ({query_tmp1}); """)
  row_to_insert = valueTuple
  curr.execute(sql_query, row_to_insert)
  # commit SQL code to execute command
  conn.commit()

#Insert new rows into the videos table using the new videos df and the insert_into_videos_table function
def append_from_df_db(curr,schemaName, tableName, df):

  column_names = df.keys().values.tolist()
  newDF = pd.DataFrame( columns=column_names)
  for i, row in df.iterrows():
    valueTuple = ()
    for column in column_names:
      valueTuple = valueTuple + (row[column],)
    insert_items_to_table(curr,schemaName, tableName,column_names,valueTuple)

"""## **Main:**
* Create database(db) on the cloud
* I am using AWS Cloud - AWS RDS Service
* Created a POSTGRES Datababse called database-artist-youtube-data
* It's a micro-instance (which is free for anyone to spin-up & play around with)

### **Connect to DB**
"""

# Connect to db
def connect_to_db(host_name, dbname, username, password, port):   
  try:
    conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
  except ps.OperationalError as e:
    raise e
  else:
    print('Connected!')
  return conn

host_name = '---'
dbname = '---'
port = '--'
username = '---'
password = '---'
conn = None
access_key_id = '----'
secret_access_key = '---'
aws_account_id = '----'

# Connecting to DB for python usage
conn = connect_to_db(host_name, dbname, username, password, port)

# cursor() method allows python code to run SQL commands in a database session

curr = conn.cursor()

"""### **Create Schemas and Tables**"""

# Call create_schema function to create a schema

create_schema(curr, "YoutubeChannel", username)

# Call create_tabe fucntion to create a videos table in the YoutubeChannel Schema 
sql_query = """
"VideoId" character varying(50) NOT NULL, 
  "SongTitle" text NOT NULL, 
  "VideoTitle" text NOT NULL,
  "VideoUploadDate" date NOT NULL,
  "ViewCount" integer NOT NULL, 
  "LikeCount" integer NOT NULL, 
  "FavoriteCount" integer NOT NULL,
  "CommentCount" integer NOT NULL, 
  PRIMARY KEY ("VideoId")
  """
create_table(curr, "YoutubeChannel", "videos", sql_query, username)

# Call create_tabe fucntion to create a analyticsGroups table in the YoutubeChannel Schema 
sql_query = """
"GroupId" character varying(50) NOT NULL,
    "GroupName" text NOT NULL,
    "NumberOfGroupItems" integer NOT NULL,
    "ReleaseDate" date NOT NULL,
    PRIMARY KEY ("GroupId")
  """
create_table(curr, "YoutubeChannel", "analyticsGroups", sql_query, username)

# Call create_tabe fucntion to create a groupItems table in the YoutubeChannel Schema 
sql_query = """
"UniqueId" character varying(100) NOT NULL,
"VideoId" character varying(50) NOT NULL,
    "GroupId" character varying(50) NOT NULL,
    "SongTitle" character varying(100) NOT NULL,
    PRIMARY KEY ("UniqueId"),
    FOREIGN KEY ("VideoId")
        REFERENCES "YoutubeChannel".videos ("VideoId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE NO ACTION,
    FOREIGN KEY ("GroupId")
        REFERENCES "YoutubeChannel".analyticsgroups ("GroupId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE NO ACTION
  """
create_table(curr, "YoutubeChannel", "groupItems", sql_query, username)

# Call create_tabe fucntion to create a groupsDailyPlays table in the YoutubeChannel Schema 
sql_query = """
"UniqueId" character varying(100) NOT NULL,
"Day" date NOT NULL,
    "GroupId" character varying(50) NOT NULL,
    "DailyViews" integer NOT NULL,
    "DailyComments" integer NOT NULL,
    "DailySubscribersGained" integer NOT NULL,
    "DailySubscribersLost" integer NOT NULL,
    "DailyAverageViewerPercentage" double precision NOT NULL,
    PRIMARY KEY ("UniqueId"),
    FOREIGN KEY ("GroupId")
        REFERENCES "YoutubeChannel".analyticsgroups ("GroupId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE NO ACTION
  """
create_table(curr, "YoutubeChannel", "groupsdailyplays", sql_query, username)

# Call create_tabe fucntion to create a dailysubscribedstatusplays table in the YoutubeChannel Schema 
sql_query = """
"UniqueId" character varying(100) NOT NULL,
"Day" date NOT NULL,
    "GroupId" character varying(50) NOT NULL,
    "SubscribedStatus" character varying(50) NOT NULL,
    "DailyViews" integer NOT NULL,
    "DailyAverageViewDuration" integer NOT NULL,
    PRIMARY KEY ("UniqueId"),
    FOREIGN KEY ("GroupId")
        REFERENCES "YoutubeChannel".analyticsgroups ("GroupId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE NO ACTION
  """
create_table(curr, "YoutubeChannel", "dailysubscribedstatusplays", sql_query, username)

# Call create_tabe fucntion to create a dailydevicetypeplays table in the YoutubeChannel Schema 
sql_query = """
"UniqueId" character varying(100) NOT NULL,
"Day" date NOT NULL,
    "GroupId" character varying(50) NOT NULL,
    "DeviceType" character varying(50) NOT NULL,
    "OperatingSystem" character varying(50) NOT NULL,
    "DailyViews" integer NOT NULL,
    "DailyEstimatedMinutesWatched" integer NOT NULL,
    PRIMARY KEY ("UniqueId"),
    FOREIGN KEY ("GroupId")
        REFERENCES "YoutubeChannel".analyticsgroups ("GroupId") MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE NO ACTION
  """
create_table(curr, "YoutubeChannel", "dailydevicetypeplays", sql_query, username)

"""### **Regular Main**

#### Update the **Videos** Table
"""

videosLifetimeStats_df = pd.read_csv('---/YouTube_Artist_Dashboard/videosLifetimeStats')
videosLifetimeStats_df = videosLifetimeStats_df.drop(columns=['Unnamed: 0',])
data_table.DataTable(videosLifetimeStats_df, include_index=False, num_rows_per_page=3)

# Update videos already existing in videos database table
new_videos_df = updateDB(curr,"YoutubeChannel", "videos", "VideoId", videosLifetimeStats_df)

#return all videos from df that aren't already in the videos database table
new_videos_df.head(5)

# Add new videos to db videos table
append_from_df_db(curr,"YoutubeChannel", "videos", new_videos_df)

# Lets see what the videos database table looks like USING SQL with Python

pullDB_videos_table = pd.read_sql("""
SELECT *
	FROM "YoutubeChannel".videos;
  """, conn, parse_dates={"VideoUploadDate": {"format": "%Y/%m/%d"}})
pullDB_videos_table.head(5)

"""#### Update the **analyticsGroups** Table"""

analyticsGroups_df = pd.read_csv('---/YouTube_Artist_Dashboard/analyticsGroups')
analyticsGroups_df = analyticsGroups_df.drop(columns=['Unnamed: 0',])
analyticsGroups_df = analyticsGroups_df.rename(columns={"groupId": "GroupId", "groupName": "GroupName", "numOf_groupItems": "NumberOfGroupItems", "releaseDate": "ReleaseDate"}, errors="raise")
data_table.DataTable(analyticsGroups_df, include_index=False, num_rows_per_page=3)

# Update groups already existing in analyticsGroups database table

new_groups_df = updateDB(curr,"YoutubeChannel", "analyticsGroups", "GroupId", analyticsGroups_df)

#return all groups from df that aren't already in the analyticsGroups database table
new_groups_df.head(5)

# Add new groups to db groups table
append_from_df_db(curr,"YoutubeChannel", "analyticsGroups", new_groups_df)

# Lets see what the analyticsGroups database table looks like USING SQL with Python

pullDB_groups_table = pd.read_sql("""
SELECT *
	FROM "YoutubeChannel".analyticsGroups;
  """, conn, parse_dates={"ReleaseDate": {"format": "%Y/%m/%d"}})
pullDB_groups_table.head(5)

"""#### Update the **groupItems** Table"""

groupItems_df = pd.read_csv('---/YouTube_Artist_Dashboard/groupItems')
groupItems_df = groupItems_df.drop(columns=['Unnamed: 0',])
groupItems_df = groupItems_df.rename(columns={"groupId": "GroupId"}, errors="raise")
groupItems_df["UniqueId"] = (groupItems_df["VideoId"] +"-"+ groupItems_df["GroupId"])
data_table.DataTable(groupItems_df, include_index=False, num_rows_per_page=3)

# Update groups already existing in groupItems database table

new_groupItems_df = updateDB(curr,"YoutubeChannel", "groupItems", "UniqueId", groupItems_df)

#return all groups from df that aren't already in the groupItems database table
new_groupItems_df.head(5)

# Add new groupItems to db groupItems table
append_from_df_db(curr,"YoutubeChannel", "groupItems", new_groupItems_df)

# Lets see what the groupItems database table looks like USING SQL with Python

pullDB_groupItems_table = pd.read_sql("""
SELECT *
	FROM "YoutubeChannel".groupItems;
  """, conn)
pullDB_groupItems_table.head(5)

"""#### Update the **groupsDailyPlays** Table

"""

groupsDailyPlays_df = pd.read_csv('---/YouTube_Artist_Dashboard/groupsDailyPlays')
groupsDailyPlays_df = groupsDailyPlays_df.drop(columns=['Unnamed: 0',])
groupsDailyPlays_df["UniqueId"] = (groupsDailyPlays_df["Day"] +"-"+ groupsDailyPlays_df["GroupId"])
data_table.DataTable(groupsDailyPlays_df, include_index=False, num_rows_per_page=3)

# Update groups already existing in groupsdailyplays database table
# updateDB(curr,schemaName, tableName, primaryKey, df)

new_groupsdailyplays_df = updateDB(curr,"YoutubeChannel", "groupsdailyplays", "UniqueId", groupsDailyPlays_df)

#return all groups from df that aren't already in the groupsdailyplays database table
new_groupsdailyplays_df.head(5)

# Add new groups to db groups table
append_from_df_db(curr,"YoutubeChannel", "groupsdailyplays", new_groupsdailyplays_df)

# Lets see what the groupsdailyplays database table looks like USING SQL with Python

pullDB_groupsdailyplays_table = pd.read_sql("""
SELECT *
	FROM "YoutubeChannel".groupsdailyplays;
  """, conn, parse_dates={"Day": {"format": "%Y/%m/%d"}})
pullDB_groupsdailyplays_table.head(5)

"""#### Update the **dailyDeviceTypePlays** Table

"""

dailyDeviceTypePlays_df = pd.read_csv('---/YouTube_Artist_Dashboard/dailyDeviceTypePlays')
dailyDeviceTypePlays_df=dailyDeviceTypePlays_df.drop(columns=['Unnamed: 0',])
dailyDeviceTypePlays_df["UniqueId"] = (dailyDeviceTypePlays_df["Day"] +"-"+ dailyDeviceTypePlays_df["GroupId"]+"-"+ dailyDeviceTypePlays_df["DeviceType"]+"-"+ dailyDeviceTypePlays_df["OperatingSystem"])
data_table.DataTable(dailyDeviceTypePlays_df, include_index=False, num_rows_per_page=3)

# Update groups already existing in dailydevicetypeplays database table

#new_dailydevicetypeplays_df = updateDB(curr,"YoutubeChannel", "dailydevicetypeplays", "UniqueId", dailyDeviceTypePlays_df)
new_dailydevicetypeplays_df = dailyDeviceTypePlays_df

#return all groups from df that aren't already in the dailydevicetypeplays database table
new_dailydevicetypeplays_df.head(5)

# Add new items to db dailydevicetypeplays table
append_from_df_db(curr,"YoutubeChannel", "dailydevicetypeplays", new_dailydevicetypeplays_df)

# Lets see what the dailydevicetypeplays database table looks like USING SQL with Python

pullDB_dailydevicetypeplays_table = pd.read_sql("""
SELECT *
	FROM "YoutubeChannel".dailydevicetypeplays;
  """, conn, parse_dates={"Day": {"format": "%Y/%m/%d"}})
pullDB_dailydevicetypeplays_table.head(5)

"""#### Update the **Subscribers** Table

"""

dailySubscribedStatusPlays_df = pd.read_csv('---/YouTube_Artist_Dashboard/dailySubscribedStatusPlays')
dailySubscribedStatusPlays_df = dailySubscribedStatusPlays_df.drop(columns=['Unnamed: 0','Unnamed: 0.1'])
dailySubscribedStatusPlays_df["UniqueId"] = (dailySubscribedStatusPlays_df["Day"] +"-"+ dailySubscribedStatusPlays_df["GroupId"]+"-"+ dailySubscribedStatusPlays_df["SubscribedStatus"])
data_table.DataTable(dailySubscribedStatusPlays_df, include_index=False, num_rows_per_page=3)

# Update groups already existing in dailysubscribedstatusplays database table

#new_dailysubscribedstatusplays_df = updateDB(curr,"YoutubeChannel", "dailysubscribedstatusplays", "UniqueId", dailySubscribedStatusPlays_df)
new_dailysubscribedstatusplays_df =  dailySubscribedStatusPlays_df

#return all groups from df that aren't already in the dailysubscribedstatusplays database table
new_dailysubscribedstatusplays_df.head(5)

# Add new groups to db dailysubscribedstatusplays table
append_from_df_db(curr,"YoutubeChannel", "dailysubscribedstatusplays", new_dailysubscribedstatusplays_df)

# Lets see what the dailysubscribedstatusplays database table looks like USING SQL with Python

pullDB_dailysubscribedstatusplays_table = pd.read_sql("""
SELECT *
	FROM "YoutubeChannel".dailysubscribedstatusplays;
  """, conn, parse_dates={"Day": {"format": "%Y/%m/%d"}})
pullDB_dailysubscribedstatusplays_table.head(5)
