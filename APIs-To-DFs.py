## **Package Installs & Library Imports**
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
## **Functions: Extracting of Data from YouTube API**


def get_video_details(videoId):
  #collecting view, like, dislike, comment counts
  # Make second API Call
  url_video_stats = "https://www.googleapis.com/youtube/v3/videos?id="+videoId+"&part=statistics&key="+API_KEY
  response_video_stats = requests.get(url_video_stats).json()

  ViewCount = response_video_stats['items'][0]['statistics']['viewCount']
  LikeCount = response_video_stats['items'][0]['statistics']['likeCount']
  FavoriteCount = response_video_stats['items'][0]['statistics']['favoriteCount']
  CommentCount = response_video_stats['items'][0]['statistics']['commentCount']
  return ViewCount,LikeCount,FavoriteCount,CommentCount

def songTitle(videoTitle):
  SongTitle = videoTitle.replace('(', '-').split("-")[1].lower()
  if 'ft' in SongTitle:
    SongTitle = SongTitle.split('ft')[0]
  return SongTitle

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

## **Functions: Extracting of Data from the YouTube Analytics API**
### **Functions:** Analytics **Groups**
# get available groups JSON response from the Youtube Analytics API
def get_groupID_analytics_API_response(pageToken,api_service_name,api_version, client_secrets_file, scopes):

  # Get credentials and create an API client
  flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
      client_secrets_file, scopes)
  Credentials = flow.run_console()

  youtube_analytics = googleapiclient.discovery.build(
      api_service_name, api_version, credentials=Credentials)

  request = youtube_analytics.groups().list(
      mine=True,
      pageToken=pageToken
    )
  response = request.execute()

  return response

# Create multiple analytics Groups
def create_multiple_analyticsGroups(listOfGroupNames, contentDetails_itemType, api_service_name,api_version, client_secrets_file, scope):
  lengthOflist = len(listOfGroupNames)
  thisItem_number = 0
  for groupName in listOfGroupNames:
    thisItem_number += 1
    snippet_title = groupName
    create_group_analytics_API_response(lengthOflist, thisItem_number, contentDetails_itemType, snippet_title, api_service_name,api_version, client_secrets_file, scope)

# create a group & get a JSON response of available groups from the Youtube Analytics API
def create_group_analytics_API_response(lengthOflist, thisItem_number, contentDetails_itemType,snippet_title,api_service_name,api_version, client_secrets_file, scopes):

  # Get credentials and create an API client
  flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
      client_secrets_file, scopes)
  Credentials = flow.run_console()

  youtube_analytics = googleapiclient.discovery.build(
      api_service_name, api_version, credentials=Credentials)

  request = youtube_analytics.groups().insert(
      body={
          "contentDetails": {
            "itemType": contentDetails_itemType
          },
          "snippet": {
            "title": snippet_title
          }
        }
    )


  response = request.execute()
  print("This group: ",snippet_title," has been created")
  print("So far .... ",thisItem_number, " out of ", lengthOflist," completed")
  return response

# multiple analytics groups
def deleteMultiple_groups(items_to_delete, api_service_name,api_version, client_secrets_file, scope):
  lengthOflist = len(items_to_delete)
  thisItem_number = 0
  for groupToDelete_Id in items_to_delete:
    thisItem_number += 1
    delete_group_analytics_API_response(lengthOflist, thisItem_number,groupToDelete_Id, api_service_name,api_version, client_secrets_file, scope)

# delete a group & get a JSON response of available groups from the Youtube Analytics API
def delete_group_analytics_API_response(lengthOflist, thisItem_number,groupId, api_service_name,api_version, client_secrets_file, scopes):

  # Get credentials and create an API client
  flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
      client_secrets_file, scopes)
  Credentials = flow.run_console()

  youtube_analytics = googleapiclient.discovery.build(
      api_service_name, api_version, credentials=Credentials)

  request = youtube_analytics.groups().delete(
        id=groupId
    )
  response = request.execute()
  print("This group ID: ",groupId," has been deleted")
  print("So far .... ",thisItem_number, " out of ", lengthOflist," completed")
  return response

#Wrapper function to call the insert_groupItem_analytics_API for insert various groupItems
def insert_Multi_groupItems(group_Id, vid_ids, api_service_name,api_version, client_secrets_file, scopes):
  lengthOflist = len(vid_ids)
  thisItem_number = 0
  for vid_id in vid_ids:
    thisItem_number += 1
    response = insert_groupItem_analytics_API(lengthOflist, thisItem_number,group_Id, vid_id, api_service_name,api_version, client_secrets_file, scopes)
  return response

# Insert a groupItem into a group in Youtube Analytics API
def insert_groupItem_analytics_API(lengthOflist, thisItem_number,group_Id, vid_id, api_service_name,api_version, client_secrets_file, scopes):

  # Get credentials and create an API client
  flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
      client_secrets_file, scopes)
  Credentials = flow.run_console()

  youtube_analytics = googleapiclient.discovery.build(
      api_service_name, api_version, credentials=Credentials)
  
  request = youtube_analytics.groupItems().insert(
      body={
        "groupId": group_Id,
        "resource": {
          "id": vid_id }
          }
          )
  response = request.execute()
  print("This ",vid_id, " video ID has been inserted into this group ID ",group_Id)
  print("So far .... ",thisItem_number, " out of ", lengthOflist," completed")
  return response

# delete a groupItem into a group in Youtube Analytics API
def delete_groupItem_analytics_API(group_Id, vid_id, api_service_name,api_version, client_secrets_file, scopes):

  # Get credentials and create an API client
  flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
      client_secrets_file, scopes)
  Credentials = flow.run_console()

  youtube_analytics = googleapiclient.discovery.build(
      api_service_name, api_version, credentials=Credentials)
  
  request = youtube_analytics.groupItems().delete(
        id=str(f"{group_Id}|{vid_id}")
    )
  response = request.execute()
  print("This ",vid_id, " video ID has been deleted from this group ID ",group_Id)
  return response

# get a list of groupItems from Youtube Analytics API
def get_groupItems_list_analytics_API(group_Id, api_service_name,api_version, client_secrets_file, scopes):

  # Get credentials and create an API client
  flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
      client_secrets_file, scopes)
  Credentials = flow.run_console()

  youtube_analytics = googleapiclient.discovery.build(
      api_service_name, api_version, credentials=Credentials)
  
  request = youtube_analytics.groupItems().list(
        groupId=group_Id
    )
  response = request.execute()
  return response

### **Shared Functions**

# Determine a start and end day
def get_end_date(start_date, daysAfterUpload):
  starting_date = datetime.strptime(start_date, '%Y-%m-%d')
  end_date = str(starting_date + timedelta(days=daysAfterUpload)).split(' ')[0]
  return end_date

# get a JSON response from the Youtube Analytics API
def get_analytics_API_response(filters,ids,dimensions,end_date, metrics,sort,start_date, api_service_name,api_version, client_secrets_file, scopes):

  # Get credentials and create an API client
  flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
      client_secrets_file, scopes)
  Credentials = flow.run_console()

  youtube_analytics = googleapiclient.discovery.build(
      api_service_name, api_version, credentials=Credentials)

  request = youtube_analytics.reports().query(
      dimensions=dimensions,
      endDate=end_date,
      ids=ids,
      metrics=metrics,
      filters = filters,
      sort=sort,
      startDate=start_date
  )


  response = request.execute()
  return response

### **Functions**: Daily Plays adjusted for **Individual Analytical Groups**
# Wrapper function to create the groupsDailyPlays_df from a JSON file
def create_groupsDailyPlays_df (group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes):
  dimensions="day"
  ids="channel==MINE"
  metrics="views,comments,subscribersGained,subscribersLost,averageViewPercentage"
  sort="day"
  groupsDailyPlays_df = pd.DataFrame(columns=["Day","GroupId","DailyViews","DailyComments","DailySubscribersGained","DailySubscribersLost","DailyAverageViewerPercentage"]) 
  groupsDailyPlays_df = get_groupsDailyPlays_df(dimensions,ids,metrics, sort, groupsDailyPlays_df, group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes)

  return groupsDailyPlays_df

# Wrapper function to call all other functions for Converting Daily Activity JSON to pandas DF
def get_groupsDailyPlays_df(dimensions,ids,metrics, sort, groupsDailyPlays_df, group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes):
  count = 0
  numberOfItems = len(listOfGroupNames)
  for groupName in listOfGroupNames:
    id = group_titleAndIds_fromJSON[groupName]
    filters = f"group=={id}"
    start_date = analyticsGroups_release_dates[groupName]
    end_date = get_end_date(start_date, daysAfterUpload)
    response_JSON = get_analytics_API_response(filters,ids,dimensions,end_date, metrics,sort,start_date, api_service_name,api_version, client_secrets_file, scopes)
    groupsDailyPlays_df = daily_JSON_to_DF(response_JSON,groupsDailyPlays_df,id)
    count+=1
    print("Added",groupName,"to the dataframe")
    print("So far ...",count,"out of",numberOfItems,"done!")

  return groupsDailyPlays_df

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

"""### **Functions:** Daily Plays Adjusted for **Individual Groups & Subscriber Status**"""

# Wrapper function to create the create_dailySubscribedStatusPlays_df from a JSON file
def create_dailySubscribedStatusPlays_df (group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes):
  dimensions="day,subscribedStatus"
  ids="channel==MINE"
  metrics="views,averageViewDuration"
  sort="day"
  dailySubscribedStatusPlays_df = pd.DataFrame(columns=["Day","GroupId","SubscribedStatus","DailyViews","DailyAverageViewDuration"]) 
  dailySubscribedStatusPlays_df = get_dailySubscribedStatusPlays_df(dimensions,ids,metrics, sort, dailySubscribedStatusPlays_df, group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes)

  return dailySubscribedStatusPlays_df

# Wrapper function to call all other functions for Converting Daily Activity JSON to pandas DF
def get_dailySubscribedStatusPlays_df(dimensions,ids,metrics, sort, dailySubscribedStatusPlays_df, group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes):
  count = 0
  numberOfItems = len(listOfGroupNames)
  for groupName in listOfGroupNames:
    id = group_titleAndIds_fromJSON[groupName]
    filters = f"group=={id}"
    start_date = analyticsGroups_release_dates[groupName]
    end_date = get_end_date(start_date, daysAfterUpload)
    response_JSON = get_analytics_API_response(filters,ids,dimensions,end_date, metrics,sort,start_date, api_service_name,api_version, client_secrets_file, scopes)
    dailySubscribedStatusPlays_df = subscribed_JSON_to_DF(response_JSON,dailySubscribedStatusPlays_df,id)
    count+=1
    print("Added",groupName,"to the dataframe")
    print("So far ...",count,"out of",numberOfItems,"done!")

  return dailySubscribedStatusPlays_df

#Saving daily activity data in JSON to pandas dataframe
def subscribed_JSON_to_DF(daily_JSON,daily_DF, id):
  if daily_JSON['kind']== "youtubeAnalytics#resultTable":
    for group in daily_JSON['rows']:
      Day = group[0]
      GroupId = id
      SubscribedStatus = group[1]
      DailyViews = group[2]
      DailyAverageViewDuration = group[3]

      #save data in pandas df
      daily_DF = daily_DF.append({ "Day":Day,"GroupId": GroupId,"SubscribedStatus": SubscribedStatus,
                      "DailyViews": DailyViews,"DailyAverageViewDuration":DailyAverageViewDuration}, ignore_index=True )
  else:
    kind = daily_JSON['kind']
    daily_DF = f"This JSON responce kind was not youtubeAnalytics#resultTable, it was {kind}"
  
  return daily_DF

### **Functions:** Daily Plays Adjusted for **Individual Groups & DeviceType**

# Wrapper function to create the create_dailySubscribedStatusPlays_df from a JSON file
def create_dailyDeviceTypePlays_df (group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes):
  dimensions="day,deviceType,operatingSystem"
  ids="channel==MINE"
  metrics="views,estimatedMinutesWatched"
  sort="day"
  dailyDeviceTypePlays_df = pd.DataFrame(columns=["Day","GroupId","DeviceType","OperatingSystem","DailyViews","DailyEstimatedMinutesWatched"]) 
  dailyDeviceTypePlays_df = get_dailyDeviceTypePlays_df(dimensions,ids,metrics, sort, dailyDeviceTypePlays_df, group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes)

  return dailyDeviceTypePlays_df

# Wrapper function to call all other functions for Converting Daily Activity JSON to pandas DF
def get_dailyDeviceTypePlays_df(dimensions,ids,metrics, sort, dailyDeviceTypePlays_df, group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes):
  count = 0
  numberOfItems = len(listOfGroupNames)
  for groupName in listOfGroupNames:
    id = group_titleAndIds_fromJSON[groupName]
    filters = f"group=={id}"
    start_date = analyticsGroups_release_dates[groupName]
    end_date = get_end_date(start_date, daysAfterUpload)
    response_JSON = get_analytics_API_response(filters,ids,dimensions,end_date, metrics,sort,start_date, api_service_name,api_version, client_secrets_file, scopes)
    dailyDeviceTypePlays_df = deviceType_JSON_to_DF(response_JSON,dailyDeviceTypePlays_df,id)
    count+=1
    print("Added",groupName,"to the dataframe")
    print("So far ...",count,"out of",numberOfItems,"done!")

  return dailyDeviceTypePlays_df

#Saving daily subacriber status activity data in JSON to pandas dataframe
def deviceType_JSON_to_DF(dailyDevice_JSON,dailyDeviceType_DF,id):
  if dailyDevice_JSON['kind']== "youtubeAnalytics#resultTable":
    for row in dailyDevice_JSON['rows']:
      Day = row[0]
      GroupId = id
      DeviceType=row[1]
      OperatingSystem = row[2]
      DailyViews = row[3]
      DailyEstimatedMinutesWatched = row[4]
      

      #save data in pandas df
      dailyDeviceType_DF = dailyDeviceType_DF.append({ "Day":Day,"GroupId": GroupId,"DeviceType": DeviceType,"OperatingSystem": OperatingSystem,"DailyViews": DailyViews,
                      "DailyEstimatedMinutesWatched": DailyEstimatedMinutesWatched}, ignore_index=True )
  else:
    kind = dailyDevice_JSON['kind']
    dailyDeviceType_DF = f"This JSON responce kind was not youtubeAnalytics#resultTable, it was {kind}"
  
  return dailyDeviceType_DF

# **Main Calls**

### **Main:** *YouTube API*
# Grab API Keys
# Working with a Youtube API
API_KEY = "---"
CHANNEL_ID = "---"

#build our dataframe
videosLifetimeStats_df = pd.DataFrame(columns=["VideoId","SongTitle","VideoTitle","VideoUploadDate","ViewCount","LikeCount","FavoriteCount","CommentCount"]) 
videosLifetimeStats_df = get_videos(videosLifetimeStats_df)
videosLifetimeStats_df.to_csv('---/YouTube_Artist_Dashboard/videosLifetimeStats.csv')
videosLifetimeStats_df = pd.read_csv('---/YouTube_Artist_Dashboard/videosLifetimeStats')
data_table.DataTable(videosLifetimeStats_df, include_index=False, num_rows_per_page=10)

### **Main:** *YouTube Analytics API*
# Scopes
scopes = ["https://www.googleapis.com/auth/youtube.readonly"]
get_groups_scopes = ["https://www.googleapis.com/auth/yt-analytics.readonly"]
create_group_scope = ["https://www.googleapis.com/auth/youtube"]
get_groupItems_scopes = ["https://www.googleapis.com/auth/youtube.readonly",
          "https://www.googleapis.com/auth/yt-analytics.readonly"]

# Authorization
# Disable OAuthlib's HTTPS verification when running locally.
# *DO NOT* leave this option enabled in production.
os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
api_service_name = "youtubeAnalytics"
api_version = "v2"
client_secrets_file = "---/client_secret.json"

#### **Main:** Analytics GroupS' creations

#Create an analyticsGroups_release_dates dictionary
analyticsGroups_release_dates = dict({})

#add a key-value pair to the analyticsGroups_release_dates dictionary
analyticsGroups_release_dates['First Album Audios'] = "2019-02-21"
analyticsGroups_release_dates['First Album Solo Tracks'] = "2019-02-21"
analyticsGroups_release_dates['First Album Featured_Tracks'] = "2019-02-21"
analyticsGroups_release_dates['First Album Top 3 Tracks'] = "2019-02-21"
analyticsGroups_release_dates['First Album Ft. Ndaka Tracks'] = "2019-02-21"
analyticsGroups_release_dates['First Album Ft. No.1 Big Artist'] = "2019-02-21"
analyticsGroups_release_dates['First Album Ft. No.2 Big Artist'] = "2019-02-21"
analyticsGroups_release_dates['First Album Hip Hop Track'] = "2019-02-21"
analyticsGroups_release_dates['First Album Audios House Track'] = "2019-02-21"

analyticsGroups_release_dates['First Album Lyrics Video'] = "2019-08-26"
analyticsGroups_release_dates['First Album Single Audio'] = "2018-07-01"
analyticsGroups_release_dates['First Album Music Video'] = "2019-04-05"

analyticsGroups_release_dates['Second Album Audios'] = "2021-02-11"
analyticsGroups_release_dates['Second Album Solo Tracks'] = "2021-02-11"
analyticsGroups_release_dates['Second Album Featured_Tracks'] = "2021-02-11"
analyticsGroups_release_dates['Second Album Top 3 Tracks'] = "2021-02-11"
analyticsGroups_release_dates['Second Album Ft. Ndaka Tracks'] = "2021-02-11"
analyticsGroups_release_dates['Second Album Ft. No.1 Big Artist'] = "2021-02-11"
analyticsGroups_release_dates['Second Album Ft. No.2 Big Artist'] = "2021-02-11"
analyticsGroups_release_dates['Second Album Hip Hop Track'] = "2021-02-11"
analyticsGroups_release_dates['Second Album House Track'] = "2021-02-11"

analyticsGroups_release_dates['Second Album Lyrics Video'] = "2021-05-23"
analyticsGroups_release_dates['Second Album Single Audio'] = "2019-06-18"
analyticsGroups_release_dates['Second Album Single Inside Album'] = "2021-02-11"

#Create an analyticsGroups_ids dictionary
analyticsGroups_ids =dict({})

#add a key-value pair to the analyticsGroups_ids dictionary
analyticsGroups_ids['First Album Audios'] = ['---']
analyticsGroups_ids['First Album Solo Tracks'] = ['---']
analyticsGroups_ids['First Album Featured_Tracks'] = ['---']
analyticsGroups_ids['First Album Top 3 Tracks'] = ['---']
analyticsGroups_ids['First Album Ft. Ndaka Tracks'] = ['---']
analyticsGroups_ids['First Album Ft. No.1 Big Artist'] = ['---']
analyticsGroups_ids['First Album Ft. No.2 Big Artist'] = ['---']
analyticsGroups_ids['First Album Hip Hop Track'] = ['---']
analyticsGroups_ids['First Album Audios House Track'] = ['--']

analyticsGroups_ids['First Album Lyrics Video'] = ['---']
analyticsGroups_ids['First Album Single Audio'] = ['---']
analyticsGroups_ids['First Album Music Video'] = ['---']

analyticsGroups_ids['Second Album Audios'] = ['----']
analyticsGroups_ids['Second Album Solo Tracks'] = ['---']
analyticsGroups_ids['Second Album Featured_Tracks'] = ['---']
analyticsGroups_ids['Second Album Top 3 Tracks'] = ['---']
analyticsGroups_ids['Second Album Ft. Ndaka Tracks'] = ['---']
analyticsGroups_ids['Second Album Ft. No.1 Big Artist'] = ['---']
analyticsGroups_ids['Second Album Ft. No.2 Big Artist'] = ['---']
analyticsGroups_ids['Second Album Hip Hop Track'] = ['---']
analyticsGroups_ids['Second Album House Track'] = ['---']

analyticsGroups_ids['Second Album Lyrics Video'] = ['---']
analyticsGroups_ids['Second Album Single Audio'] = ['---']
analyticsGroups_ids['Second Album Single Inside Album'] = ['---']

# Creating a listOfGroupNames
listOfGroupNames = str(analyticsGroups_ids.keys()).split("[")[1].split("]")[0].replace("'","")
listOfGroupNames = listOfGroupNames.split(", ")
print(len(listOfGroupNames),'items: ', listOfGroupNames)

# Create multiple analytics groups
contentDetails_itemType = "youtube#video"
create_multiple_analyticsGroups(listOfGroupNames, contentDetails_itemType, api_service_name,api_version, client_secrets_file, create_group_scope)

# delete a group & get a JSON response of available groups from the Youtube Analytics API
items_to_delete = ['VFvaD6s0qEM']
deleteMultiple_groups(items_to_delete, api_service_name,api_version, client_secrets_file, create_group_scope)

#Get a list of JSON files with existing analytics groups
# This function that calls itself when the jSON response has a nextPageToken
JSON_List = []
pageToken=""
noNextPageToken = True
while noNextPageToken:
  groupID_analytics_API_response = get_groupID_analytics_API_response(pageToken,api_service_name,api_version, client_secrets_file, get_groups_scopes)
  JSON_List.append(groupID_analytics_API_response)
  if len(groupID_analytics_API_response['items']) == 0:
    noNextPageToken = False
  else:
    pageToken = groupID_analytics_API_response['nextPageToken']
len(JSON_List)

#Create an group_titleAndIds_fromJSON dictionary
group_titleAndIds_fromJSON = dict({})

#add a key-value pair to the group_titleAndIds_fromJSON dictionary
for response in JSON_List:
  if len(response['items']) > 0:
    for item in response['items']:
      title = item['snippet']['title']
      id = item['id']
      group_titleAndIds_fromJSON[title] = id

print("There are",len(group_titleAndIds_fromJSON), "groups in total and here are their group ids: ",group_titleAndIds_fromJSON )
print("There are",len(analyticsGroups_ids), "groups and video ids in them: ",analyticsGroups_ids )
print("There are",len(listOfGroupNames), "empty groups' names:",listOfGroupNames )

# Insert a groupItem into a group in Youtube Analytics API
# Insert multiple groupItems into multiple groups in YouTube Analytics API
# 1. for each title in listOfGroupNames
# 2. get its group Id from the group_titleAndIds_fromJSON dictionary
# 3. get it's groupItems from the analyticsGroups_ids dictionary
# 4. send it's groupId and groupItems to the insertMultipleg_roupItems Function
for groupName in listOfGroupNames:
  groupId = group_titleAndIds_fromJSON[groupName]
  groupItems = analyticsGroups_ids[groupName]
  print("The group id for",groupName, "is", groupId)
  print("Here are the group items",groupItems)
  insert_Multi_groupItems(groupId, groupItems, api_service_name,api_version, client_secrets_file, create_group_scope)

# delete a groupItem into a group in Youtube Analytics API
grou_Id_to_Delete_item_from = ""
vid_id_to_delete = ""
delete_groupItem_analytics_API(grou_Id_to_Delete_item_from, vid_id_to_delete, api_service_name,api_version, client_secrets_file, create_group_scope)

# get all available groups and a JSON of their list of groupItems from Youtube Analytics API
for groupName in group_titleAndIds_fromJSON.keys():
  groupId = group_titleAndIds_fromJSON[groupName]
  print("The group id for",groupName, "is", groupId)
  items_inGroup = get_groupItems_list_analytics_API(groupId, api_service_name,api_version, client_secrets_file, get_groupItems_scopes)
  print("Here are the items in this group:",items_inGroup)

#### **Main:** Analytics Tables' Creation

print("There are",len(group_titleAndIds_fromJSON), "groups in total and here are their group ids: ",group_titleAndIds_fromJSON )
print("There are",len(analyticsGroups_ids), "groups and here are the videos' ids that are each group:",analyticsGroups_ids )
print("There are",len(listOfGroupNames), "groups and here are their names:",listOfGroupNames)

# Creating a groupsDailyPlays_df from JSON response files from the Youtube Analytics API and saving it to a csv file 
daysAfterUpload = 365
groupsDailyPlays_df = create_groupsDailyPlays_df (group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes)
groupsDailyPlays_df.to_csv('---/YouTube_Artist_Dashboard/groupsDailyPlays.csv')

groupsDailyPlays_df = pd.read_csv('---/YouTube_Artist_Dashboard/groupsDailyPlays')
data_table.DataTable(groupsDailyPlays_df, include_index=False, num_rows_per_page=10)

# Creating a dailySubscribedStatusPlays_df from JSON response files from the Youtube Analytics API and saving it to a csv file 
daysAfterUpload = 365
dailySubscribedStatusPlays_df = create_dailySubscribedStatusPlays_df (group_titleAndIds_fromJSON, listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes)
dailySubscribedStatusPlays_df.to_csv('---/YouTube_Artist_Dashboard/dailySubscribedStatusPlays.csv')

dailySubscribedStatusPlays_df = pd.read_csv('---/YouTube_Artist_Dashboard/dailySubscribedStatusPlays')
data_table.DataTable(dailySubscribedStatusPlays_df, include_index=False, num_rows_per_page=10)

# Creating a dailyDeviceTypePlays_df from JSON response files from the Youtube Analytics API and saving it to a csv file 
dailyDeviceTypePlays_df = create_dailyDeviceTypePlays_df (group_titleAndIds_fromJSON,listOfGroupNames, analyticsGroups_release_dates, daysAfterUpload, api_service_name,api_version, client_secrets_file, scopes)
dailyDeviceTypePlays_df.to_csv('---/YouTube_Artist_Dashboard/dailyDeviceTypePlays.csv')

dailyDeviceTypePlays_df = pd.read_csv('---/YouTube_Artist_Dashboard/dailyDeviceTypePlays')
data_table.DataTable(dailyDeviceTypePlays_df, include_index=False, num_rows_per_page=10)

#Creating the analyticsGroups_df
analyticsGroups_df = pd.DataFrame(columns=["groupId","groupName","numOf_groupItems","releaseDate"])
for groupName in listOfGroupNames:
  groupId = group_titleAndIds_fromJSON[groupName]
  releaseDate = analyticsGroups_release_dates[groupName]
  groupItems = analyticsGroups_ids[groupName]
  numOf_groupItems = len(groupItems)
  
  #save data in pandas df
  analyticsGroups_df = analyticsGroups_df.append({ "groupId":groupId,"groupName": groupName,"numOf_groupItems": numOf_groupItems,"releaseDate":releaseDate}, ignore_index=True )

analyticsGroups_df.to_csv('---/YouTube_Artist_Dashboard/analyticsGroups.csv')

analyticsGroups_df = pd.read_csv('---/YouTube_Artist_Dashboard/analyticsGroups')
data_table.DataTable(analyticsGroups_df, include_index=False, num_rows_per_page=10)

#Creating the groupItems_df
groupItems_df = pd.DataFrame(columns=["VideoId","groupId","SongTitle"])

for index, row in videosLifetimeStats_df.iterrows():
  VideoId = row['VideoId']
  SongTitle = row['SongTitle']
  notFound = True
  for groupName in listOfGroupNames:
    itemList = analyticsGroups_ids[groupName]
    if VideoId in itemList:
      groupId = group_titleAndIds_fromJSON[groupName]
      #save data in pandas df
      groupItems_df = groupItems_df.append({ "VideoId":VideoId,"groupId": groupId,"SongTitle": SongTitle}, ignore_index=True )

groupItems_df.to_csv('---/YouTube_Artist_Dashboard/groupItems.csv')
groupItems_df = pd.read_csv('--/YouTube_Artist_Dashboard/groupItems')
data_table.DataTable(groupItems_df, include_index=False, num_rows_per_page=10)
