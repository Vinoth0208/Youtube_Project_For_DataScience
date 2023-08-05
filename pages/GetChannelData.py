import json

import pandas as pd
import pymongo
from pymongo.server_api import ServerApi
import mysql.connector

from DataCollection import *
import streamlit as st


st.set_page_config(
    page_title="YouTube Data Harvesting and Warehousing",
)
st.header("Youtbube Data Harvesting and Warehousing")
st.text("""This page lets you to paste a youtube channel id and
get the basic informations of the channel and on click of a 
button you can add your data to Mongo DB Atlas Data lake""")
channel_id=st.text_input("channel_id")
state = st.button("Get Data")
channel_informations={}

def callfunctionstocollectData(channel_id):
    ChannelData = collectDataFromYoutubeForChannels(channel_id)
    channel_informations = {
        "Channel_Name": {
            'channel_name': ChannelData['items'][0]['snippet']['title'],
            'channel_id': ChannelData['items'][0]['id'],
            "subscription_Count": ChannelData['items'][0]['statistics']['subscriberCount'],
            "channel_views": ChannelData['items'][0]['statistics']['viewCount'],
            'channel_description': ChannelData['items'][0]['snippet']['description'],
            'playlists_id': ChannelData['items'][0]['contentDetails']['relatedPlaylists']['uploads']}
        }
    playlistId=channel_informations['Channel_Name']['playlists_id']
    playlistData=collectDataFromYoutubeForPlaylist(playlistId)
    video_id={}
    for i in range(50):
        video_id[f'videoId_{i}']=playlistData['items'][i]['contentDetails']['videoId']

    videoData={}
    commentData={}
    for j in range(50):
        videoData[f'videoData_{j}']=collectDataFromYoutubeForVideo(video_id[f'videoId_{j}'])
    for m in range(50):
        commentData[f'comment_{m}'] =collectDataFromYoutubeForVideoComments(video_id[f'videoId_{m}'])
    commentDataList={}
    for n in range(50):
        length=len(commentData[f'comment_{n}']["items"])
        commentDatalistinternal={}
        for o in range(length):
            commentDatalistinternal[f"Comment_Id_{o}"] = {
                                                "Comment_Id":commentData[f'comment_{n}']["items"][o]["id"],
                                                "Comment_Text":commentData[f'comment_{n}']["items"][o]["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                                "Comment_Author":commentData[f'comment_{n}']["items"][o]["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                                "Comment_PublishedAt":commentData[f'comment_{n}']["items"][o]["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                                                "Video_Id":commentData[f'comment_{n}']["items"][o]["snippet"]["videoId"]
                                                }
        commentDataList[f'comments_{n}']=commentDatalistinternal

    videoDataList={}
    for k in range(50):
        videoDataList[f"video_Id_{k}"]={
            "Video_Id": videoData[f'videoData_{k}']['items'][0]["id"],
            "Video_Name":videoData[f'videoData_{k}']['items'][0]["snippet"]["title"],
            "Video_Description":videoData[f'videoData_{k}']['items'][0]["snippet"]["description"],
            "Tags":videoData[f'videoData_{k}']['items'][0]["snippet"]["tags"],
            "PublishedAt":videoData[f'videoData_{k}']['items'][0]["snippet"]["publishedAt"],
            "View_Count":videoData[f'videoData_{k}']['items'][0]["statistics"]["viewCount"],
            "Like_Count":videoData[f'videoData_{k}']['items'][0]["statistics"]["likeCount"],
            # "Dislike_Count":
            "Favorite_Count":videoData[f'videoData_{k}']['items'][0]["statistics"]["favoriteCount"],
            "Comment_Count":videoData[f'videoData_{k}']['items'][0]["statistics"]["commentCount"],
            "Duration":videoData[f'videoData_{k}']['items'][0]["contentDetails"]["duration"],
            "Thumbnail":videoData[f'videoData_{k}']['items'][0]["snippet"]["thumbnails"]["default"]["url"],
            "Caption_Status":videoData[f'videoData_{k}']['items'][0]["contentDetails"]["caption"],
            "playlistId":channel_informations["Channel_Name"]["playlists_id"],
            "comments":commentDataList[f'comments_{k}']

        }

    channel_informations.update(videoDataList)

    return channel_informations


if "ChState" not in st.session_state:
  st.session_state.ChState = False
if state or st.session_state.ChState :
    st.session_state.ChState=True
    st.session_state.channel_informations=callfunctionstocollectData(channel_id)
    channelName=st.session_state.channel_informations['Channel_Name']['channel_name']
    channel_id=st.session_state.channel_informations['Channel_Name']['channel_id']
    subscription_Count=st.session_state.channel_informations['Channel_Name']['subscription_Count']
    channel_views=st.session_state.channel_informations['Channel_Name']['channel_views']
    channel_description=st.session_state.channel_informations['Channel_Name']['channel_description']
    st.subheader(":blue[Channel Name:]")
    st.write(f':red[{channelName}]')
    st.subheader(":blue[Channel Id:]")
    st.write(f':red[{channel_id}]')
    st.subheader(":blue[Subscription_Count:]")
    st.write(f':red[{subscription_Count}]')
    st.subheader(":blue[Channel_views:]")
    st.write(f':red[{channel_views}]')
    st.subheader(":blue[Channel_description:]")
    st.write(f':red[{channel_description}]')

    UploadState = st.button("Upload Data to Mongo DB")
    if UploadState:
        client = pymongo.MongoClient(
                "mongodb+srv://cluster0.45tmhd1.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority",
                tls=True, tlsCertificateKeyFile='C:\\Users\\Vinoth\\Downloads\\X509-cert-5084476662482489136.pem',
                server_api=ServerApi('1'))
        db = client["youtube"]
        db.YTData.insert_one(st.session_state.channel_informations)
        st.write(f':green[{st.session_state.channel_informations["Channel_Name"]["channel_name"]} Inserted into Mongo DB]')


    channelOptions = pymongo.MongoClient(
                "mongodb+srv://cluster0.45tmhd1.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority",
                tls=True, tlsCertificateKeyFile='C:\\Users\\Vinoth\\Downloads\\X509-cert-5084476662482489136.pem',
                server_api=ServerApi('1'))
    db = channelOptions["youtube"]
    s=db.YTData.find({},{"Channel_Name":1,"_id":0})
    opt=[]
    for i in s:
        opt.append(i["Channel_Name"]["channel_name"])


    selected=st.selectbox("Select a Channel Name to migrate data to SQL:",opt)

    channelOptions = pymongo.MongoClient(
                            "mongodb+srv://cluster0.45tmhd1.mongodb.net/?authSource=%24external&authMechanism=MONGODB-X509&retryWrites=true&w=majority",
                            tls=True, tlsCertificateKeyFile='C:\\Users\\Vinoth\\Downloads\\X509-cert-5084476662482489136.pem',
                            server_api=ServerApi('1'))
    db = channelOptions["youtube"]
    ss = db.YTData.find({"Channel_Name.channel_name":selected}, {"_id": 0})
    for j in ss:
                df = pd.DataFrame.from_dict(j)
                # df.rename(columns={"Channel_Name":"ChannelData"},inplace=True)
                dff=df[df["Channel_Name"].notnull()]
                ChannelDf=dff["Channel_Name"]
                vdf=df.drop(columns="Channel_Name")
                vdff=vdf.dropna()
                vdfff=vdff[:-1]
                cf=df[-1:]
                ccf=cf.drop(columns="Channel_Name")
                ccf = ccf.reset_index(drop=True)
                for i in range(50):
                    cdf=pd.DataFrame.from_dict(ccf)
                cmt=[]
                for i in range(50):
                    commdf=pd.DataFrame(cdf.transpose()[0][f'video_Id_{i}'])
                    cs=commdf.transpose().values.tolist()
                    if(cs==[]):
                        continue
                    else:
                        cmt.append(cs)
                pdf=df.filter(items=['channel_id', 'playlists_id'], axis=0)
                pdf.transpose()

                cd=ChannelDf.to_list()
                pd=pdf["Channel_Name"].to_list()

                vd=vdfff.transpose().values.tolist()
                for i in range(50):
                    vd[i][3]=",".join(vd[i][3])

migrateState = st.button("Migrate Data to SQL")
if migrateState:
        myConnection=mysql.connector.connect(host="localhost",user="root",password="root")
        mycursor = myConnection.cursor()

        mycursor.execute("CREATE DATABASE if not exists youtube")
        mycursor.execute("use youtube")
        mycursor.execute(
            "Create Table if not exists Channel(channel_name varchar(1000),channel_id varchar(255),subscription_count INT,channel_views INT,channel_description varchar(1000),playlists_id varchar(255)  ,PRIMARY KEY (channel_id))")
        mycursor.execute(
            "Create Table if not exists Playlist(channel_id varchar(255),playlists_id varchar(255),PRIMARY KEY (playlists_id),FOREIGN KEY (channel_id) REFERENCES Channel(channel_id))")
        mycursor.execute(
            "Create Table if not exists Video(video_id varchar(255),video_name varchar(1000),"
            "video_description text(65535),tags varchar(1000),published_at varchar(1000),view_count INT, "
            "like_count INT,fav_count INT,comment_count INT, duration varchar(1000),thumbnail varchar(1000),caption_status varchar(1000),playlists_id varchar(255),PRIMARY KEY (video_id))")
        mycursor.execute(
            "Create Table if not exists Comment(comment_id varchar(255),comment_text varchar(1000),comment_author varchar(1000),"
            "published_at varchar(1000),video_id varchar(255),PRIMARY KEY (comment_id))")
        sqlch="insert into Channel(channel_name,channel_id,subscription_count,channel_views,channel_description,playlists_id)values(%s,%s,%s,%s,%s,%s)"
        valuesch=tuple(cd)
        sqlpd="insert into Playlist(channel_id,playlists_id)values(%s,%s)"
        valuespd=tuple(pd)
        for i in range(len(vd)):
            valuesvd = tuple(vd[i])
            sqlvd="insert into Video(video_id,video_name,video_description,tags,published_at,view_count,like_count,fav_count,comment_count,duration,thumbnail,caption_status,playlists_id)values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            mycursor.execute(sqlvd, valuesvd)
            myConnection.commit()


        for i in range(len(cmt)):
            for j in range(len(cmt[i])):
                valuescd = tuple(cmt[i][j])

            sqlcd="insert into Comment(comment_id ,comment_text ,comment_author ,published_at ,video_id )values(%s,%s,%s,%s,%s)"
            mycursor.execute(sqlcd, valuescd)
            myConnection.commit()
        mycursor.execute(sqlch,valuesch)
        mycursor.execute(sqlpd, valuespd)
        myConnection.commit()
        st.write(f":green[{selected} Migrated to SQL]")