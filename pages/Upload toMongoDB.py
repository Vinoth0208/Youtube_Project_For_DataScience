import isodate
import pandas as pd
import pymongo
from pymongo.server_api import ServerApi
import mysql.connector

from DataCollection import *
import streamlit as st

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
                vd[i][9] = isodate.parse_duration(vd[i][9]).total_seconds()

migrateState = st.button("Migrate Data to SQL")
if migrateState:
        myConnection=mysql.connector.connect(host="localhost",user="root",password="root")
        mycursor = myConnection.cursor()

        mycursor.execute("CREATE DATABASE if not exists youtube")
        mycursor.execute("use youtube")
        mycursor.execute(
            "Create Table if not exists Channel(channel_name varchar(1000),channel_id varchar(255),subscription_count long,channel_views long,channel_description varchar(1000),playlists_id varchar(255), video_count INT ,PRIMARY KEY (channel_id))")
        mycursor.execute(
            "Create Table if not exists Playlist(channel_id varchar(255),playlists_id varchar(255),PRIMARY KEY (playlists_id),FOREIGN KEY (channel_id) REFERENCES Channel(channel_id))")
        mycursor.execute(
            "Create Table if not exists Video(video_id varchar(255),video_name varchar(1000),"
            "video_description text(65535),tags varchar(1000),published_at varchar(1000),view_count INT, "
            "like_count INT,fav_count INT,comment_count INT, duration varchar(1000),thumbnail varchar(1000),caption_status varchar(1000),playlists_id varchar(255),PRIMARY KEY (video_id))")
        mycursor.execute(
            "Create Table if not exists Comment(comment_id varchar(255),comment_text varchar(1000),comment_author varchar(1000),"
            "published_at varchar(1000),video_id varchar(255),PRIMARY KEY (comment_id))")
        sqlch="insert into Channel(channel_name,channel_id,subscription_count,channel_views,channel_description,playlists_id,video_count)values(%s,%s,%s,%s,%s,%s,%s)"
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
