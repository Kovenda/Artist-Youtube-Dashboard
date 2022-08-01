# Data Exploration in SQL
install.packages("dplyr")
install.packages("gridExtra")

postgresql://username:password@hostname/dbname

# Lets see what the videos database table looks
SELECT *
	FROM "YoutubeChannel".videos
  LIMIT 2;
 
 # Lets see what the analyticsgroups database table looks
 SELECT *
	FROM "YoutubeChannel".analyticsgroups
  LIMIT 2;
  
# Lets see what the groupitems database table looks
SELECT *
	FROM "YoutubeChannel".groupitems
  LIMIT 2;
  
# Lets see what the groupsdailyplays database table looks 
SELECT *
	FROM "YoutubeChannel".groupsdailyplays
  LIMIT 2;
  
# Question: Find the video that has a SongTilte that starts with himba which contains the word lyrics in it's VideoTitle
# Schema: YoutubeChannel Table: dailydevicetypeplays

SELECT *
FROM "YoutubeChannel".videos AS v
WHERE
  v."SongTitle" ILIKE '%himba%'  AND
  v."VideoTitle" ILIKE '%Lyrics%'

# Question: Find the SongTitles and ViewCount of the official audios where the duo featured the artist Ndaka
# Schema: YoutubeChannel Table: videos

SELECT v."SongTitle", v."ViewCount"
FROM "YoutubeChannel".videos as v
WHERE 
  v."VideoTitle" ILIKE '%Ndaka%' AND
  v."VideoTitle" ILIKE '%official audio%'

# Question: Find all the analyticsgroups that has a GroupName that starts with "First Album"
# Schema: YoutubeChannel Table: analyticsgroups

SELECT ag."GroupName", ag."NumberOfGroupItems", ag."ReleaseDate"
FROM "YoutubeChannel".analyticsgroups as ag
WHERE 
  ag."GroupName" ILIKE 'First Album%'

# Question: Find all the analyticsgroups that has a GroupName that starts with "Second Album"
# Schema: YoutubeChannel Table: analyticsgroups

SELECT ag."GroupName", ag."NumberOfGroupItems", ag."ReleaseDate"
FROM "YoutubeChannel".analyticsgroups as ag
WHERE 
  ag."GroupName" ILIKE 'Second Album%'
  
# Question: Find the SongTitles and ViewCounts of all the videos that are in the First Album Audios analytics group in the analyticsgroups table
SELECT v."SongTitle", v."ViewCount"
FROM "YoutubeChannel".videos AS v 
WHERE v."VideoId" IN (
  SELECT items."VideoId"
  FROM "YoutubeChannel".groupItems AS items
  JOIN "YoutubeChannel".analyticsgroups AS groups
    ON items."GroupId" = groups."GroupId"
    WHERE groups."GroupName" = 'First Album Audios' )
order by v."ViewCount" desc

# Question: Find the SongTitles and ViewCounts of all the videos that are in the First Album Solo Tracks analytics group in the analyticsgroups table
SELECT v."SongTitle", v."ViewCount"
FROM "YoutubeChannel".videos AS v 
WHERE v."VideoId" IN (
  SELECT items."VideoId"
  FROM "YoutubeChannel".groupItems AS items
  JOIN "YoutubeChannel".analyticsgroups AS groups
    ON items."GroupId" = groups."GroupId"
    WHERE groups."GroupName" = 'First Album Solo Tracks' )
order by v."ViewCount" desc

# Question: Find the SongTitles and ViewCounts of all the videos that are in the First Album Featured_Tracks analytics group in the analyticsgroups table
SELECT v."SongTitle", v."ViewCount"
FROM "YoutubeChannel".videos AS v 
WHERE v."VideoId" IN (
  SELECT items."VideoId"
  FROM "YoutubeChannel".groupItems AS items
  JOIN "YoutubeChannel".analyticsgroups AS groups
    ON items."GroupId" = groups."GroupId"
    WHERE groups."GroupName" = 'First Album Featured_Tracks' )
order by v."ViewCount" desc

# Question: Find the SongTitles and ViewCounts of all the videos that are in the Second Album Audios analytics group in the analyticsgroups table
SELECT v."SongTitle", v."ViewCount"
FROM "YoutubeChannel".videos AS v 
WHERE v."VideoId" IN (
  SELECT items."VideoId"
  FROM "YoutubeChannel".groupItems AS items
  JOIN "YoutubeChannel".analyticsgroups AS groups
    ON items."GroupId" = groups."GroupId"
    WHERE groups."GroupName" = 'Second Album Audios' )
order by v."ViewCount" desc

# Question: Find the SongTitles and ViewCounts of all the videos that are in the Second Album Solo Tracks analytics group in the analyticsgroups table
SELECT v."SongTitle", v."ViewCount"
FROM "YoutubeChannel".videos AS v 
WHERE v."VideoId" IN (
  SELECT items."VideoId"
  FROM "YoutubeChannel".groupItems AS items
  JOIN "YoutubeChannel".analyticsgroups AS groups
    ON items."GroupId" = groups."GroupId"
    WHERE groups."GroupName" = 'Second Album Solo Tracks' )
order by v."ViewCount" desc

# Question: Find the SongTitles and ViewCounts of all the videos that are in the Second Album Featured_Tracks analytics group in the analyticsgroups table
SELECT v."SongTitle", v."ViewCount"
FROM "YoutubeChannel".videos AS v 
WHERE v."VideoId" IN (
  SELECT items."VideoId"
  FROM "YoutubeChannel".groupItems AS items
  JOIN "YoutubeChannel".analyticsgroups AS groups
    ON items."GroupId" = groups."GroupId"
    WHERE groups."GroupName" = 'Second Album Featured_Tracks' )
order by v."ViewCount" desc

# Question: The % of views⏯ that came from each device for the first 30 days after release dates
SELECT devicePlays_outer."DeviceType",devicePlays_outer."OperatingSystem", (sum(devicePlays_outer."DailyViews")/sum(devicePlays_outer."total_dailyviews"))*100 AS percent_of_views
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT DevicePlayTotals."totals_day", DevicePlayTotals."totals_id",devicePlaysOG."DeviceType",devicePlaysOG."OperatingSystem",DevicePlayTotals."total_dailyviews", devicePlaysOG."DailyViews"
  FROM "YoutubeChannel".dailydevicetypeplays AS devicePlaysOG
  JOIN (
    SELECT devicePlays."Day" AS totals_day, devicePlays."GroupId" AS totals_id, SUM(devicePlays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailydevicetypeplays AS devicePlays
    GROUP BY devicePlays."Day", devicePlays."GroupId") AS DevicePlayTotals 
    ON 
      devicePlaysOG."Day" = DevicePlayTotals."totals_day" AND
      devicePlaysOG."GroupId" = DevicePlayTotals."totals_id"
) AS devicePlays_outer
  ON 
    groups."GroupId" = devicePlays_outer."totals_id"
WHERE 
  devicePlays_outer."totals_day" < groups."ReleaseDate" + 31 
GROUP BY devicePlays_outer."DeviceType",devicePlays_outer."OperatingSystem"
HAVING (sum(devicePlays_outer."DailyViews")/sum(devicePlays_outer."total_dailyviews")) > 0.04
ORDER BY percent_of_views desc

# Question: All Android mobile views for each group 30 days after release:
SELECT groups."GroupName", sum(devicePlays_outer."DailyViews") AS group_android_mobile_views
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT DevicePlayTotals."totals_day", DevicePlayTotals."totals_id",devicePlaysOG."DeviceType",devicePlaysOG."OperatingSystem",DevicePlayTotals."total_dailyviews", devicePlaysOG."DailyViews"
  FROM "YoutubeChannel".dailydevicetypeplays AS devicePlaysOG
  JOIN (
    SELECT devicePlays."Day" AS totals_day, devicePlays."GroupId" AS totals_id, SUM(devicePlays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailydevicetypeplays AS devicePlays
    GROUP BY devicePlays."Day", devicePlays."GroupId") AS DevicePlayTotals 
    ON 
      devicePlaysOG."Day" = DevicePlayTotals."totals_day" AND
      devicePlaysOG."GroupId" = DevicePlayTotals."totals_id"
) AS devicePlays_outer
  ON 
    groups."GroupId" = devicePlays_outer."totals_id"
WHERE 
  devicePlays_outer."DeviceType" = 'MOBILE' AND 
  devicePlays_outer."OperatingSystem" = 'ANDROID' AND
  devicePlays_outer."totals_day" < groups."ReleaseDate" + 31 
GROUP BY groups."GroupName"
ORDER BY group_android_mobile_views desc

# Question: Top 3 groups with the highest % of views⏯ that came from Android phones for the first 30 days after release date
SELECT groups."GroupName", (sum(devicePlays_outer."DailyViews")/sum(devicePlays_outer."total_dailyviews"))*100 AS percent_of_android_mobile_views
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT DevicePlayTotals."totals_day", DevicePlayTotals."totals_id",devicePlaysOG."DeviceType",devicePlaysOG."OperatingSystem",DevicePlayTotals."total_dailyviews", devicePlaysOG."DailyViews"
  FROM "YoutubeChannel".dailydevicetypeplays AS devicePlaysOG
  JOIN (
    SELECT devicePlays."Day" AS totals_day, devicePlays."GroupId" AS totals_id, SUM(devicePlays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailydevicetypeplays AS devicePlays
    GROUP BY devicePlays."Day", devicePlays."GroupId") AS DevicePlayTotals 
    ON 
      devicePlaysOG."Day" = DevicePlayTotals."totals_day" AND
      devicePlaysOG."GroupId" = DevicePlayTotals."totals_id"
) AS devicePlays_outer
  ON 
    groups."GroupId" = devicePlays_outer."totals_id"
WHERE 
  devicePlays_outer."DeviceType" = 'MOBILE' AND 
  devicePlays_outer."OperatingSystem" = 'ANDROID' AND
  devicePlays_outer."totals_day" < groups."ReleaseDate" + 31 
GROUP BY groups."GroupName"
ORDER BY percent_of_android_mobile_views desc
limit 3

# Final Table: Join the analyticsgroups table and the dailydevicetypeplays table for further analysis
SELECT groups."GroupName", groups."NumberOfGroupItems", dt_OS."DeviceType", dt_OS."OperatingSystem", groups."ReleaseDate", dt_OS."Day", dt_OS."DailyEstimatedMinutesWatched",dt_OS."total_dailyviews", dt_OS."DailyViews"
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT *
  FROM "YoutubeChannel".dailydevicetypeplays AS devicePlaysOG
  JOIN (
    SELECT devicePlays."Day" AS totals_day, devicePlays."GroupId" AS totals_id, SUM(devicePlays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailydevicetypeplays AS devicePlays
    GROUP BY devicePlays."Day", devicePlays."GroupId") AS DevicePlayTotals 
    ON 
      devicePlaysOG."Day" = DevicePlayTotals."totals_day" AND
      devicePlaysOG."GroupId" = DevicePlayTotals."totals_id"
) AS dt_OS
  ON 
    groups."GroupId" = dt_OS."totals_id"
    
    
# Question: The % of views⏯ that came from susbcribers and non-subscribers for the first 30 days after release dates
SELECT subsplays_outer."SubscribedStatus", (sum(subsplays_outer."DailyViews")/sum(subsplays_outer."total_dailyviews"))*100 AS percent_of_views
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT subsplays_totals."totals_day", subsplays_totals."totals_id",subsplays_og."SubscribedStatus",subsplays_totals."total_dailyviews", subsplays_og."DailyViews"
  FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays_og
  JOIN (
    SELECT subsplays."Day" AS totals_day, subsplays."GroupId" AS totals_id, SUM(subsplays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays
    GROUP BY subsplays."Day", subsplays."GroupId") AS subsplays_totals 
    ON 
      subsplays_og."Day" = subsplays_totals."totals_day" AND
      subsplays_og."GroupId" = subsplays_totals."totals_id"
) AS subsplays_outer
  ON 
    groups."GroupId" = subsplays_outer."totals_id"
WHERE 
  subsplays_outer."totals_day" < groups."ReleaseDate" + 31 
GROUP BY subsplays_outer."SubscribedStatus"
ORDER BY percent_of_views desc

# Question: The % of views⏯ that came from susbcribers and non-subscribers for the first 30 days after release dates for anything associated with first album
SELECT subsplays_outer."SubscribedStatus", (sum(subsplays_outer."DailyViews")/sum(subsplays_outer."total_dailyviews"))*100 AS percent_of_views
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT subsplays_totals."totals_day", subsplays_totals."totals_id",subsplays_og."SubscribedStatus",subsplays_totals."total_dailyviews", subsplays_og."DailyViews"
  FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays_og
  JOIN (
    SELECT subsplays."Day" AS totals_day, subsplays."GroupId" AS totals_id, SUM(subsplays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays
    GROUP BY subsplays."Day", subsplays."GroupId") AS subsplays_totals 
    ON 
      subsplays_og."Day" = subsplays_totals."totals_day" AND
      subsplays_og."GroupId" = subsplays_totals."totals_id"
) AS subsplays_outer
  ON 
    groups."GroupId" = subsplays_outer."totals_id"
WHERE 
  subsplays_outer."totals_day" < groups."ReleaseDate" + 31 AND
  groups."GroupName" ILIKE 'First Album%'
GROUP BY subsplays_outer."SubscribedStatus"
ORDER BY percent_of_views desc

# Question: The % of views⏯ that came from susbcribers and non-subscribers for the first 30 days after release dates for anything associated with second album
SELECT subsplays_outer."SubscribedStatus", (sum(subsplays_outer."DailyViews")/sum(subsplays_outer."total_dailyviews"))*100 AS percent_of_views
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT subsplays_totals."totals_day", subsplays_totals."totals_id",subsplays_og."SubscribedStatus",subsplays_totals."total_dailyviews", subsplays_og."DailyViews"
  FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays_og
  JOIN (
    SELECT subsplays."Day" AS totals_day, subsplays."GroupId" AS totals_id, SUM(subsplays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays
    GROUP BY subsplays."Day", subsplays."GroupId") AS subsplays_totals 
    ON 
      subsplays_og."Day" = subsplays_totals."totals_day" AND
      subsplays_og."GroupId" = subsplays_totals."totals_id"
) AS subsplays_outer
  ON 
    groups."GroupId" = subsplays_outer."totals_id"
WHERE 
  subsplays_outer."totals_day" < groups."ReleaseDate" + 31 AND
  groups."GroupName" ILIKE 'second album%'
GROUP BY subsplays_outer."SubscribedStatus"
ORDER BY percent_of_views desc

# Question: Total subscribed views for each group 30 days after release:
SELECT groups."GroupName", sum(subsplays_outer."DailyViews") AS group_subscriber_views
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT subsplays_totals."totals_day", subsplays_totals."totals_id",subsplays_og."SubscribedStatus",subsplays_totals."total_dailyviews", subsplays_og."DailyViews"
  FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays_og
  JOIN (
    SELECT subsplays."Day" AS totals_day, subsplays."GroupId" AS totals_id, SUM(subsplays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays
    GROUP BY subsplays."Day", subsplays."GroupId") AS subsplays_totals 
    ON 
      subsplays_og."Day" = subsplays_totals."totals_day" AND
      subsplays_og."GroupId" = subsplays_totals."totals_id"
) AS subsplays_outer
  ON 
    groups."GroupId" = subsplays_outer."totals_id"
WHERE 
  subsplays_outer."totals_day" < groups."ReleaseDate" + 31 AND
  subsplays_outer."SubscribedStatus" LIKE 'SUBSCRIBED%'
GROUP BY groups."GroupName"
ORDER BY group_subscriber_views desc

# Question: Top 3 groups with the highest proportion of views⏯ that came from UNSUBSCRIBED users for the first 30 days after release date
SELECT groups."GroupName", sum(subsplays_outer."DailyViews")/sum(subsplays_outer."total_dailyviews") AS unsubscriber_views_proportion
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT subsplays_totals."totals_day", subsplays_totals."totals_id",subsplays_og."SubscribedStatus",subsplays_totals."total_dailyviews", subsplays_og."DailyViews"
  FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays_og
  JOIN (
    SELECT subsplays."Day" AS totals_day, subsplays."GroupId" AS totals_id, SUM(subsplays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays
    GROUP BY subsplays."Day", subsplays."GroupId") AS subsplays_totals 
    ON 
      subsplays_og."Day" = subsplays_totals."totals_day" AND
      subsplays_og."GroupId" = subsplays_totals."totals_id"
) AS subsplays_outer
  ON 
    groups."GroupId" = subsplays_outer."totals_id"
WHERE 
  subsplays_outer."totals_day" < groups."ReleaseDate" + 31 AND
  subsplays_outer."SubscribedStatus" LIKE 'UN%'
GROUP BY groups."GroupName"
ORDER BY unsubscriber_views_proportion desc
LIMIT 3

# Question: Top 3 groups with the highest % of views⏯ that came from SUBSCRIBED users for the first 30 days after release date
SELECT groups."GroupName", sum(subsplays_outer."DailyViews")/sum(subsplays_outer."total_dailyviews")*100 AS subscriber_views_percent
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT subsplays_totals."totals_day", subsplays_totals."totals_id",subsplays_og."SubscribedStatus",subsplays_totals."total_dailyviews", subsplays_og."DailyViews"
  FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays_og
  JOIN (
    SELECT subsplays."Day" AS totals_day, subsplays."GroupId" AS totals_id, SUM(subsplays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays
    GROUP BY subsplays."Day", subsplays."GroupId") AS subsplays_totals 
    ON 
      subsplays_og."Day" = subsplays_totals."totals_day" AND
      subsplays_og."GroupId" = subsplays_totals."totals_id"
) AS subsplays_outer
  ON 
    groups."GroupId" = subsplays_outer."totals_id"
WHERE 
  subsplays_outer."totals_day" < groups."ReleaseDate" + 31 AND
  subsplays_outer."SubscribedStatus" LIKE 'SUB%'
GROUP BY groups."GroupName"
ORDER BY subscriber_views_percent desc
LIMIT 3

# Final Table: Join the analyticsgroups table and the dailydevicetypeplays table for further analysis
SELECT groups."GroupName", groups."NumberOfGroupItems", groups."ReleaseDate", subsplays_outer."Day", subsplays_outer."SubscribedStatus", subsplays_outer."total_dailyviews", subsplays_outer."DailyAverageViewDuration", subsplays_outer."DailyViews"
FROM "YoutubeChannel".analyticsgroups AS groups
JOIN (
  SELECT *
  FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays_og
  JOIN (
    SELECT subsplays."Day" AS totals_day, subsplays."GroupId" AS totals_id, SUM(subsplays."DailyViews") AS total_dailyviews 
    FROM "YoutubeChannel".dailysubscribedstatusplays AS subsplays
    GROUP BY subsplays."Day", subsplays."GroupId") AS subsplays_totals 
    ON 
      subsplays_og."Day" = subsplays_totals."totals_day" AND
      subsplays_og."GroupId" = subsplays_totals."totals_id"
) AS subsplays_outer
  ON 
    groups."GroupId" = subsplays_outer."totals_id"








