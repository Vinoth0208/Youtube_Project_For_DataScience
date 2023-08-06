import isodate as isodate
import pandas as pd
import pymongo
from pymongo.server_api import ServerApi
import mysql.connector

from DataCollection import *
import streamlit as st



op=['What are the names of all the videos and their corresponding channels?',
    'Which channels have the most number of videos, and how many videos do they have?',
    'What are the top 10 most viewed videos and their respective channels?',
    'How many comments were made on each video, and what are their corresponding video names?',
    'Which videos have the highest number of likes, and what are their corresponding channel names?',
    'What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
    'What is the total number of views for each channel, and what are their corresponding channel names?',
    'What are the names of all the channels that have published videos in the year 2022?',
    'What is the average duration of all videos in each channel, and what are their corresponding channel names?',
    'Which videos have the highest number of comments, and what are their corresponding channel names?'
    ]

choosen=st.selectbox('Choose a query to fetch data', op, key='yek')
myConnection = mysql.connector.connect(host="localhost", user="root", password="root")
mycursor = myConnection.cursor()
mycursor.execute("use youtube;")
#q1
if choosen=='What are the names of all the videos and their corresponding channels?':
    sqlQuery='Select video_name , channel_name from video join playlist on video.playlists_id = playlist.playlists_id join channel on  playlist.playlists_id=channel.playlists_id'
    mycursor.execute(sqlQuery)
    q1rs=mycursor.fetchall()
    q1lis=[]
    for i in range (len(q1rs)):
       c={
          "Video_Name":q1rs[i][0],
          "Channel_Name": q1rs[i][1]
       }
       q1lis.append(c)
    q1df=pd.DataFrame.from_dict(q1lis)
    st.write(q1df)
# q2
elif choosen=='Which channels have the most number of videos, and how many videos do they have?':
    mycursor.execute('set @x=(select max(video_count) from channel);')
    mycursor.execute('select channel_name, video_count from channel where video_count=@x')
    rs=mycursor.fetchall()
    cn=rs[0][0]
    st.write(":orange[channel_name:] "+cn)
    st.write(f":orange[video_count:] {rs[0][1]}")

# #q3
elif choosen =='What are the top 10 most viewed videos and their respective channels?':
    mycursor.execute('select video_name, view_count, channel_name from video join playlist on video.playlists_id=playlist.playlists_id join channel on channel.playlists_id=playlist.playlists_id order by view_count desc limit 10')
    rs=mycursor.fetchall()
    q3lis=[]
    for i in range(len(rs)):
        rset={
            "Video_Name": rs[i][0],
            "View_count":rs[i][1],
            "Channel_Name": rs[i][2]

        }
        q3lis.append(rset)
    q3df = pd.DataFrame.from_dict(q3lis)
    st.write(q3df)

#q4
elif choosen =='How many comments were made on each video, and what are their corresponding video names?':
    mycursor.execute('select video_name, comment_count, channel_name from video join playlist on video.playlists_id=playlist.playlists_id join channel on channel.playlists_id=playlist.playlists_id order by comment_count desc')
    rs=mycursor.fetchall()
    q4lis = []
    for i in range(len(rs)):
        rset = {
            "Video_Name": rs[i][0],
            "comment_count": rs[i][1],
            "Channel_Name": rs[i][2]

        }
        q4lis.append(rset)
    q4df = pd.DataFrame.from_dict(q4lis)
    st.write(q4df)

#q5
elif choosen =='Which videos have the highest number of likes, and what are their corresponding channel names?':
    mycursor.execute('select channel_name, video_name, max(like_count) as like_count from channel  join playlist on channel.playlists_id=playlist.playlists_id join video on video.playlists_id=playlist.playlists_id group by channel_name')
    rs=mycursor.fetchall()
    q5lis=[]
    for i in range(len(rs)):
        rset = {
            "Channel_Name": rs[i][0],
            "Video_Name": rs[i][1],
            "Like_count": rs[i][2]

        }
        q5lis.append(rset)
    q5df = pd.DataFrame.from_dict(q5lis)
    st.write(q5df)
#q6
elif choosen =='What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
    mycursor.execute('SELECT video_name, like_count FROM youtube.video order by like_count desc;')
    rs = mycursor.fetchall()
    q6lis = []
    for i in range(len(rs)):
        rset = {
            "Video_Name": rs[i][0],
            "Like_count": rs[i][1]

        }
        q6lis.append(rset)
    q6df = pd.DataFrame.from_dict(q6lis)
    st.write(q6df)
#q7
elif choosen == 'What is the total number of views for each channel, and what are their corresponding channel names?':
    mycursor.execute('SELECT channel_name, channel_views FROM youtube.channel;')
    rs = mycursor.fetchall()
    q7lis = []
    for i in range(len(rs)):
        rset = {
            "Channel_name": rs[i][0],
            "Views_count": rs[i][1]

        }
        q7lis.append(rset)
    q7df = pd.DataFrame.from_dict(q7lis)
    st.write(q7df)

#q8
elif choosen == 'What are the names of all the channels that have published videos in the year 2022?':
    mycursor.execute("SELECT video_name, channel_name, published_at FROM youtube.video join playlist on video.playlists_id=playlist.playlists_id join channel on channel.playlists_id=playlist.playlists_id where published_at like '%2022%'")
    rs = mycursor.fetchall()
    q8lis = []
    for i in range(len(rs)):
        rset = {
            "Video_name": rs[i][0],
            "Channel_name": rs[i][1],
            "PublishedAt":rs[i][2]

        }
        q8lis.append(rset)
    q8df = pd.DataFrame.from_dict(q8lis)
    st.write(q8df)

#q9
elif choosen =='What is the average duration of all videos in each channel, and what are their corresponding channel names?':
    mycursor.execute(
        'select avg(duration),channel_name from video join playlist on playlist.playlists_id=video.playlists_id join channel on channel.playlists_id=playlist.playlists_id group by channel_name')
    rs = mycursor.fetchall()
    q9lis = []
    for i in range(len(rs)):
        rset = {
            "Channel_name": rs[i][1],
            "Average duration in seconds": rs[i][0]

        }
        q9lis.append(rset)
    q9df = pd.DataFrame.from_dict(q9lis)
    st.write(q9df)

#q10
elif choosen =='Which videos have the highest number of comments, and what are their corresponding channel names?':
    mycursor.execute('select channel_name, video_name, max(comment_count) as comment_count from channel join playlist on channel.playlists_id=playlist.playlists_id join video on video.playlists_id=playlist.playlists_id group by channel_name')
    rs = mycursor.fetchall()
    q10lis = []
    for i in range(len(rs)):
        rset = {
            "Channel_name": rs[i][0],
            "Video_name": rs[i][1],
            "Comment_count": rs[i][2]

        }
        q10lis.append(rset)
    q10df = pd.DataFrame.from_dict(q10lis)
    st.write(q10df)
