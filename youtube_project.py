import streamlit as st
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import pandas as pd
import isodate

# Function to connect to YouTube API
def API_connect():
    API_KEY = 'AIzaSyDEzs3axFvW_KAjHrU1NZrGNonNGnr-dNM' # 1st key
    return build('youtube', 'v3', developerKey=API_KEY)

def get_channel_info(youtube, channel_name):
    try:
        search_response = youtube.search().list(
            part="snippet",
            type="channel",
            maxResults=1,
            q=channel_name
        ).execute()

        if not search_response['items']:
            return None 
        
        channel_id = search_response['items'][0]['snippet']['channelId']
        
        response = youtube.channels().list(
            part='snippet,statistics,contentDetails',
            id=channel_id
        ).execute()
        return response['items'][0]
    except HttpError as e:
        st.error("An error occurred while fetching channel details:")
        st.error(e)
        return None

def get_video_details_with_comments(youtube,channel_id):
    try:
        Videos = []
        next_page_token = None
        while True:
            response = youtube.search().list(
                part = "snippet",
                channelId = channel_id,
                maxResults = 50,
                pageToken = next_page_token
            ).execute()
            for item in response.get('items',[]):
                    if 'id' in item and 'videoId' in item['id']:
                        video_id = item['id']['videoId']
                        Videos.append(video_id)
            next_page_token = response.get('nextPageToken')
            if next_page_token is None:
                 break
        return Videos
    except Exception as e:
        st.error("An error occurred while fetching video details")
        st.error(e)
        return []
    
def video_info_with_comments(youtube,video_id):
    try:
        video_response = youtube.videos().list(
            part = "snippet,statistics,contentDetails",
            id = video_id
        ).execute()
        if not video_response['items']:
            st.error(f"Video not found: {video_id}")
            return {}
        video_snippet = video_response['items'][0]["snippet"]
        video_statistics = video_response['items'][0]['statistics']
        video_contentDetails = video_response['items'][0]['contentDetails']
        if video_snippet.get("commentsDisabled",False):
            st.error(f"Comments are disabled for this video: {video_id}")
            return {}
        comments = fetch_comments(youtube,video_id)
        video_info = {
            'video_id': video_id,
            'channel_id' : video_snippet.get('channelId',''),
            'title': video_snippet.get('title',''),
            'description': video_snippet.get('description', ''),
            'tags' : video_snippet.get('tags',''),
            'publish_At': video_snippet.get('publishedAt',''),
            'duration' : video_contentDetails.get('duration',''),
            'viewCount': int(video_statistics.get('viewCount', 0)),
            'likeCount': int(video_statistics.get('likeCount', 0)),
            'commentCount': int(video_statistics.get('commentCount', 0)),
            'comments': comments
        }
        return video_info
    except HttpError as e:
        st.error(f"An error occurred with video ID: {video_id}")
        st.error(e)
        return {}

def fetch_comments(youtube,video_id):
    try:
        comments = []
        request = youtube.commentThreads().list(
            part = "snippet",
            videoId = video_id
        )
        while request:
            comments_response =  request.execute()
            for comment_item in comments_response.get('items',[]):
                comment = {
                    'commet_id' : comment_item['snippet']['topLevelComment']['id'],
                    'channel_id' : comment_item['snippet']['channelId'],
                    'video_id': comment_item['snippet']['videoId'],
                    'author_name': comment_item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'text': comment_item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'like_count': int(comment_item['snippet']['topLevelComment']['snippet']['likeCount']),
                    'publish_At' : comment_item['snippet']['topLevelComment']['snippet']['publishedAt']
                }
                comments.append(comment)
            request = youtube.commentThreads().list_next(request, comments_response)
        return comments
    except HttpError as e:
        if e.resp.status == 403:
            st.error(f"Comments are disabled for this video: {video_id}")
        else:
            st.error(f"An error occurred while fetching comments for video ID: {video_id}")
            st.error(e)
        return []

def connection_to_mongodb(get_channel_info,finall_out_put):
    try:
        uri_1 = "mongodb://localhost:27017/"
        client = MongoClient(uri_1)
        db = client['Youtube_project']
        collection = db['channel_details']
        data = [{
            "channal_details" : get_channel_info,
            "video_information"  : finall_out_put
        }]
        collection.insert_many(data)
        st.success("successfully data insert into mongoDb")
    except Exception as e:
        st.error("An error occurred while insesting data into MongoDB:")
        st.error(e)

def convert_to_mysql_datetime(iso_timestamp):
    """Function to convert ISO 8601 timestamp to MySQL DATETIME format"""
    try:
        # Attempt to parse timestamp with milliseconds
        if '.' in iso_timestamp:
            formatted_timestamp = datetime.strptime(iso_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        else:
            formatted_timestamp = datetime.strptime(iso_timestamp, "%Y-%m-%dT%H:%M:%S%z")
        # Format datetime object into MySQL DATETIME format (YYYY-MM-DD HH:MM:SS)
        mysql_timestamp = formatted_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        return mysql_timestamp
    except ValueError as e:
        st.error(f"Error converting timestamp: {e}")
        return None

def sql_isodate_convert(youtube_duration):
    duration = str(isodate.parse_duration(youtube_duration))
    return duration

def connection_to_mysql():
    """Imsert of data into mysql"""
    try:
        uri_1 = "mongodb://localhost:27017/"
        client = MongoClient(uri_1)
        db = client['Youtube_project']
        collection = db['channel_details']
        mysql_conn = mysql.connector.connect(
            host = "127.0.0.1",
            user = "root",
            password = "Maram@123",
            database = "youtube_datas"
        )
        if mysql_conn.is_connected():
            cursor = mysql_conn.cursor()
        # Fetch the last document from MongoDB
        last_document = collection.find_one(sort=[("_id",-1)])
        if last_document:
                # Insert into channels table
                channel_details = last_document.get("channal_details",'')
                channel_id = channel_details.get("channel_id",'')
                channel_name = channel_details.get("channel_name",'')
                channel_thumbnail = channel_details.get("channel_thumbnail",'')
                channel_description = channel_details.get("channel_description",'')
                playlists = channel_details.get("playlists",'')
                subscriber_count = int(channel_details.get("subscriber_count",0))
                view_count = int(channel_details.get("view_count",0))
                video_count = int(channel_details.get("video_count",0))
                published_at = convert_to_mysql_datetime(channel_details.get("published_at",''))
                channel_query = """INSERT IGNORE INTO channel_details (channel_id, channel_name, channel_thumbnail, channel_description, playlists, subscriber_count, view_count, video_count, published_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                channel_values = (channel_id, channel_name, channel_thumbnail, channel_description, playlists, subscriber_count, view_count, video_count, published_at)
                cursor.execute(channel_query, channel_values)
                
                # Insert into videos table
                for video in last_document.get("video_information",''):
                    video_id = video.get("video_id",'')
                    title = video.get("title",'')
                    description = video.get("description",'')
                    publish_at = convert_to_mysql_datetime(video.get("publish_At",''))
                    duration = sql_isodate_convert(video.get('duration',''))
                    view_count = int(video.get("viewCount",''))
                    like_count = int(video.get("likeCount",''))
                    comment_count = int(video.get("commentCount",''))

                    video_query = """INSERT IGNORE INTO videos_details (video_id, channel_id, title, description, publish_at, duration, view_count, like_count, comment_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                    video_values = (video_id, channel_id, title, description, publish_at, duration, view_count, like_count, comment_count)
                    cursor.execute(video_query, video_values)

                    # Insert into comments table
                    for comment in video.get("comments",''):
                        comment_id = comment.get("commet_id",'')
                        author_name = comment.get("author_name",'')
                        text = comment.get("text",'')
                        like_count = int(comment.get("like_count",0))
                        comment_publish_at = convert_to_mysql_datetime(comment.get("publish_At",''))

                        comment_query = """INSERT IGNORE INTO comments_details (comment_id, video_id, author_name, text, like_count, publish_at) VALUES (%s, %s, %s, %s, %s, %s)"""
                        comment_values = (comment_id, video_id, author_name, text, like_count, comment_publish_at)
                        cursor.execute(comment_query, comment_values)

                mysql_conn.commit()
                st.success("successully data insert into Mysql database")
    except Error as e:
        st.error(f"Error: {e}")
    finally:
        if mysql_conn.is_connected():
            cursor.close()
            mysql_conn.close()
            st.success("MySQL connection is closed")

# Connect to MySQL
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="Maram@123",
            database="youtube_datas"
        )
        return conn
    except mysql.connector.Error as e:
        st.error(f"Error connecting to MySQL: {e}")
        return None

# Function to fetch data from MySQL based on a query
def fetch_query(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    return pd.DataFrame(result, columns=columns)

def Function_to_fetch_data():
    """Function to fetch data from MySQL based on a query"""
    conn = connect_to_mysql()
    if conn is None:
        return None
    else:
        # Query 1: Names of all videos and their corresponding channels
        st.subheader("1. Names of all videos and their corresponding channels")
        query_1 = """
            SELECT v.title AS video_name, c.channel_name
            FROM videos_details v
            JOIN channel_details c ON v.channel_id = c.channel_id
        """
        df_1 = fetch_query(conn, query_1)
        st.dataframe(df_1)

        # Query 2: Channels with the most number of videos
        st.subheader("2. Channels with the most number of videos")
        query_2 = """
            SELECT c.channel_name, COUNT(v.video_id) AS video_count
            FROM channel_details c
            JOIN videos_details v ON c.channel_id = v.channel_id
            GROUP BY c.channel_name
            ORDER BY video_count DESC LIMIT 1
        """
        df_2 = fetch_query(conn, query_2)
        st.dataframe(df_2)

        # Query 3: Top 10 most viewed videos and their respective channels
        st.subheader("3. Top 10 most viewed videos and their respective channels")
        query_3 = """
            SELECT v.title AS video_name, c.channel_name, v.view_count
            FROM videos_details v
            JOIN channel_details c ON v.channel_id = c.channel_id
            ORDER BY v.view_count DESC
            LIMIT 10
        """
        df_3 = fetch_query(conn, query_3)
        st.dataframe(df_3)

        # Query 4: Number of comments on each video and their corresponding video names
        st.subheader("4. Number of comments on each video and their corresponding video names")
        query_4 = """
            SELECT v.title AS video_name,    COUNT(cm.comment_id) AS comment_count
            FROM videos_details v
            JOIN comments_details cm ON v.video_id = cm.video_id
            GROUP BY v.title
        """
        df_4 = fetch_query(conn, query_4)
        st.dataframe(df_4)

        # Query 5: Videos with the highest number of likes and their corresponding channel names
        st.subheader("5. Videos with the highest number of likes and their corresponding channel names")
        query_5 = """
            SELECT v.title AS video_name, c.channel_name, v.like_count
            FROM videos_details v
            JOIN channel_details c ON v.channel_id = c.channel_id
            ORDER BY v.like_count DESC
            LIMIT 10
        """
        df_5 = fetch_query(conn, query_5)
        st.dataframe(df_5)

        # Query 6: Total number of likes and dislikes for each video
        st.subheader("6. Total number of likes for each video")
        query_6 = """
            SELECT v.title AS video_name, v.like_count
            FROM videos_details v
        """
        df_6 = fetch_query(conn, query_6)
        st.dataframe(df_6)

        # Query 7: Total number of views for each channel
        st.subheader("7. Total number of views for each channel")
        query_7 = """
            SELECT c.channel_name, SUM(v.view_count) AS total_views
            FROM channel_details c
            JOIN videos_details v ON c.channel_id = v.channel_id
            GROUP BY c.channel_name
        """
        df_7 = fetch_query(conn, query_7)
        st.dataframe(df_7)

        # Query 8: Names of all the channels that published videos in the year 2022
        st.subheader("8. Channels that published videos in 2022")
        query_8 = """
            SELECT DISTINCT c.channel_name
            FROM channel_details c
            JOIN videos_details v ON c.channel_id = v.channel_id
            WHERE YEAR(v.publish_at) = 2022
        """
        df_8 = fetch_query(conn, query_8)
        st.dataframe(df_8)

        # Query 9: Average duration of all videos in each channel
        st.subheader("9. Average duration of all videos in each channel")
        query_9 = """
            SELECT c.channel_name, SEC_TO_TIME(AVG(TIME_TO_SEC(v.duration))) AS average_duration
            FROM channel_details c
            JOIN videos_details v ON c.channel_id = v.channel_id
            GROUP BY c.channel_name
        """
        df_9 = fetch_query(conn, query_9)
        st.dataframe(df_9)

        # Query 10: Videos with the highest number of comments and their corresponding channel names
        st.subheader("10. Videos with the highest number of comments and their corresponding channel names")
        query_10 = """
            SELECT v.title AS video_name, c.channel_name, COUNT(cm.comment_id) AS comment_count
            FROM videos_details v
            JOIN comments_details cm ON v.video_id = cm.video_id
            JOIN channel_details c ON v.channel_id = c.channel_id
            GROUP BY v.title, c.channel_name
            ORDER BY comment_count DESC
            LIMIT 10
        """
        df_10 = fetch_query(conn, query_10)
        st.dataframe(df_10)

        # Close the connection
        conn.close()

def main():
    st.title("YouTube Data Analysis")
    search_query = st.text_input("Enter youtube channel name:")
    if search_query:
        youtube = API_connect()
        if youtube:
            channel_info = get_channel_info(youtube,search_query)
            if channel_info:
                channel_snippet = channel_info.get('snippet', {})
                channel_statistics = channel_info.get('statistics', {})
                channel_id = channel_info.get('id', '')
                channel_name = channel_snippet.get('title', '')
                channel_description = channel_snippet.get('description', '')
                playlists = channel_info.get('contentDetails', {}).get('relatedPlaylists', {}).get('uploads', '')
                subscriber_count = channel_statistics.get('subscriberCount', 0)
                view_count = channel_statistics.get('viewCount', 0)
                video_count = channel_statistics.get('videoCount', 0)
                published_at = channel_snippet.get('publishedAt', '')
                channel_thumbnail = channel_snippet.get('thumbnails', {}).get('medium', {}).get('url', '')
                # Write Streamlit display
                st.image(channel_thumbnail, caption=None, width=None, use_column_width=None, clamp=False, channels="RGB", output_format="auto")
                st.write(f"Channel Name : {channel_name}")
                st.write(f"Channel ID : {channel_id}")
                st.write(f"Channel Description : {channel_description}")
                st.write(f"Channel Playlists : {playlists}")
                st.write(f"Subscriber Count : {subscriber_count}")
                st.write(f"View Count : {view_count}")
                st.write(f"Video Count : {video_count}")
                st.write(f"Published At : {published_at}")
                # Now create data
                channel_info = {
                    'channel_thumbnail': channel_thumbnail,
                    'channel_name': channel_name,
                    'channel_id': channel_id,
                    'channel_description': channel_description,
                    'playlists': playlists,
                    'subscriber_count': int(subscriber_count),
                    'view_count': int(view_count),
                    'video_count': int(video_count),
                    'published_at': published_at
                }
                # progress bar
                my_bar = st.progress(0, text="Fetching video details...")
                # Collecting videos and comments
                video_with_comments = []
                video_list = get_video_details_with_comments(youtube, channel_id)
                total_videos = len(video_list)
                # Update progress bar while fetching video details
                for i,video_id in enumerate(video_list):
                    get_info_video_details = video_info_with_comments(youtube,video_id)
                    video_with_comments.append(get_info_video_details)
                    my_bar.progress((i + 1) / total_videos, text=f"Fetching video details: {i + 1}/{total_videos}")
                mongo_conn = connection_to_mongodb(channel_info, video_with_comments)
                mysql_conn = connection_to_mysql()
                finall_output = Function_to_fetch_data()
                st.success("All data's are collected successfully")

if __name__ == "__main__":
    main()