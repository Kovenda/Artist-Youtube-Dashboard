# Artist-Youtube-Dashboard
I built an artist Tableau Dashboard, data pulled from youtube using Youtube API and Youtube Analytics API and stored in PostgreSQL database on AWS-RDS

## **Project Components**
1.   Working with real data
2.   Working with modern Technologies (**API**s:'Youtube' &  **Databases in the cloud**: AWS Cloud)
3.   Building models
4.   Making an impact/ getting validation

**1. APIs: What to get**

1. ***Skills gained from working with an API***
 *   *Real time updates*
 *   *Date and timestamps for each record*
 *   *Geolocations*
 *   *Numbers and text for data analysis*


2. ***Skills gained from working with an API***
 *   *Setup and configure APIs (e.g dealing with API tokens*
 *   *Use Python libraries to make API calls*
 *   *Work with data structures like JSON and dictionaries to help collect and save data from the APIs*

**2. Databases in the cloud (Externsive Display of SQL skills)**
1. Building a pipeline to clean and add new records from API to database in the cloud
2. Building a data pipeline with a cloud provide like AWS or Google Cloud

**3. Building Models: Qs to Answer**
1. Why did you pick that model? 
 * What are you trying to accomplish with this model, that you can't with others?
2. How did you clean your data?
 * Why did you clean it that way?
3. What type of validation tests did you perform on the data to prepare it for the mode?
4. What are the assumptions of your model?
 * How did you validate those Assumptions?
5. How did you optimize your model?
 * What were the trade-off decisions that you made?
6. How did you implement your test/ control?
7. How does the underlying Maths in your model works?

**4. Building Models: Qs to Answer**
1. Put your code in ***github*** repos and data science sub-reddits
2. Create Visualizations in ***Tableau***
3. Share your Tableau Visualizations on ***Tableau Public***

## **Functions: Extracting of Data from YouTube API**
``` {.python}
def get_videos(df):
  #Make first API Call
  pageToken = ""
  url = "https://www.googleapis.com/youtube/v3/search?key="+API_KEY+"&channelId="+CHANNEL_ID+"&part=snippet,id&order=date&maxResults=10000&"+pageToken

  response = requests.get(url).json()
  time.sleep(1)

  for video in response['items']:
    if video['id']['kind']=="youtube#video":
      VideoId = video['id']['videoId']
      VideoTitle = str(video['snippet']['title']).replace('&amp;',',')
      SongTitle = songTitle(VideoTitle)
      VideoUploadDate = str(video['snippet']['publishedAt']).split("T")[0]

      #collecting view, like, dislike, comment counts
      ViewCount,LikeCount,FavoriteCount,CommentCount = get_video_details(VideoId)

      #save data in pandas df
      df = df.append({ "VideoId":VideoId,"SongTitle": SongTitle,"VideoTitle": VideoTitle,
                      "VideoUploadDate": VideoUploadDate,"ViewCount":ViewCount,
                      "LikeCount":LikeCount,"FavoriteCount":FavoriteCount,"CommentCount":CommentCount}, ignore_index=True )
  return df
``` 
*   Use **Python Requests Library** to make an **API** Call
*   Make an API Call to **Youtube API**
``` {.python}
API_KEY = "---"
CHANNEL_ID = "---"
videosLifetimeStats_df = get_videos(videosLifetimeStats_df)
``` 
*   Collect Data as **JSON**
``` {.python}
url = "https://www.googleapis.com/youtube/v3/search?key="+API_KEY+"&channelId="+CHANNEL_ID+"&part=snippet,id&order=date&maxResults=10000&"+pageToken
response = requests.get(url).json()
``` 
*   Save Data into a Pandas dataframe
``` {.python}
#build our dataframe
videosLifetimeStats_df = pd.DataFrame(columns=["VideoId","SongTitle","VideoTitle","VideoUploadDate","ViewCount","LikeCount","FavoriteCount","CommentCount"])
#save data in pandas df
df = df.append({ "VideoId":VideoId,"SongTitle": SongTitle,"VideoTitle": VideoTitle,
                "VideoUploadDate": VideoUploadDate,"ViewCount":ViewCount,
                "LikeCount":LikeCount,"FavoriteCount":FavoriteCount,"CommentCount":CommentCount}, ignore_index=True )
``` 

## **Functions: Extracting of Data from the YouTube Analytics API**
``` {.python}
def create_groupsDailyPlays_df (group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes):
  dimensions="day"
  ids="channel==MINE"
  metrics="views,comments,subscribersGained,subscribersLost,averageViewPercentage"
  sort="day"
  groupsDailyPlays_df = pd.DataFrame(columns=["Day","GroupId","DailyViews","DailyComments","DailySubscribersGained","DailySubscribersLost","DailyAverageViewerPercentage"]) 
  groupsDailyPlays_df = get_groupsDailyPlays_df(dimensions,ids,metrics, sort, groupsDailyPlays_df, group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes)
``` 

*   Use **Python Requests Library** to make an **API** Call
*   Make an API Call to **Youtube Analytics API**
``` {.python}
# Scopes
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
get_groups_scopes = ["https://www.googleapis.com/auth/yt-analytics.readonly"]
create_group_scope = ["https://www.googleapis.com/auth/youtube"]
get_groupItems_scopes = ["https://www.googleapis.com/auth/youtube.readonly",
          "https://www.googleapis.com/auth/yt-analytics.readonly"]
          
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
api_service_name = "youtubeAnalytics"
api_version = "v2"
client_secrets_file = "---/client_secret.json"

dailySubscribedStatusPlays_df = create_dailySubscribedStatusPlays_df (group_titleAndIds_fromJSON, listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes)
``` 

*   Collect Data as **JSON** and save Data into a Pandas dataframe
``` {.python}
#Saving daily activity data in JSON to pandas dataframe
def daily_JSON_to_DF(daily_JSON,daily_DF, id):
  if daily_JSON['kind']== "youtubeAnalytics#resultTable":
    for group in daily_JSON['rows']:
      Day = group[0]
      GroupId = id
      DailyViews = group[1]
      DailyComments = group[2]
      DailySubscribersGained = group[3]
      DailySubscribersLost = group[4]
      DailyAverageViewerPercentage = group[5]

      #save data in pandas df
      daily_DF = daily_DF.append({ "Day":Day,"GroupId": GroupId,"DailyViews": DailyViews,
                      "DailyComments": DailyComments,"DailySubscribersGained":DailySubscribersGained,"DailySubscribersLost":DailySubscribersLost,"DailyAverageViewerPercentage":DailyAverageViewerPercentage}, ignore_index=True )
  else:
    kind = daily_JSON['kind']
    daily_DF = f"This JSON responce kind was not youtubeAnalytics#resultTable, it was {kind}"
  
  return daily_DF
``` 

# **Functions: Saving data from DFs to Tables on an AWS RDS Database instance on AWS Cloud**
*   Save data in Pandas  to Database in **AWS Cloud**
*   Saving data in the **cloud** solves **memory** and **performance** issues
*   This enables **saving** of different **cleaned up versions** of our **data** in the **database** 
*   This **eliminates** the need to **reclean** the data everytime I run an **analysis**
*   **Pullling** and **Pushing Data** to a **database** is a **useful** skill to have as a Data **Analyst**/ **Scientist**
*   Will **UPDATE** the same **database TABLE** with **knewly retrieved data** from the **API**

## Function: Create a SCHEMA for POSTGRES Database in Python using SQL commands
``` {.python}
def create_schema(curr, schemaName, username):
  create_schema_command = (f"""
  CREATE SCHEMA IF NOT EXISTS "{schemaName}"
    AUTHORIZATION {username}; 
  """)

  curr.execute(create_schema_command)
  # commit SQL code to execute command
  conn.commit()
```
## Function: Connect to db
``` {.python}
def connect_to_db(host_name, dbname, username, password, port):   
  try:
    conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)
  except ps.OperationalError as e:
    raise e
  else:
    print('Connected!')
  return conn
``` 

## Connect to DB
``` {.python}
conn = connect_to_db(host_name, dbname, username, password, port)
curr = conn.cursor()

``` 

## Create DB Components
``` {.python}
# Create a Schema
create_schema(curr, "YoutubeChannel", username)

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
```

## Function: Updating DB Tables
``` {.python}

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
  return newDF

# Update videos already existing in videos database table
new_videos_df = updateDB(curr,"YoutubeChannel", "videos", "VideoId", videosLifetimeStats_df)

``` 







