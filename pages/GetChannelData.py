import json

import pandas as pd
import pymongo
from pymongo.server_api import ServerApi

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
                                                }
        commentDataList[f'comments_{n}']=commentDatalistinternal

    videoDataList={}
    for k in range(50):
        videoDataList[f"video_Id_{k}"]={
            "Video_Id": videoData[f'videoData_{0}']['items'][0]["id"],
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
            "comments":commentDataList[f'comments_{k}']
        }

    channel_informations.update(videoDataList)

    return channel_informations


if "ChState" not in st.session_state:
  st.session_state.ChState = False
if state or st.session_state.ChState :
    st.session_state.ChState=True
    st.session_state.channel_informations=callfunctionstocollectData(channel_id)
    st.caption(":blue[Channel Name:]")
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
        st.write(st.session_state.channel_informations)


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
    migrateState = st.button("Migrate Data to SQL")
    if migrateState:
        ss = db.YTData.find({"Channel_Name.channel_name":selected}, {"_id": 0})
        for j in ss:
            df = pd.DataFrame.from_dict(j)
            dff=df[df["Channel_Name"].notnull()]
            ChannelDf=dff["Channel_Name"]
            st.write(ChannelDf)
            vdf=df.drop(columns="Channel_Name")
            vdff=vdf.dropna()
            vdfff=vdff[:-1]
            st.write(vdfff.transpose)