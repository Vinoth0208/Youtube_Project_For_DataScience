from googleapiclient.discovery import build

api_key = open("api.txt").read()
youtube = build('youtube', 'v3', developerKey=api_key)
def collectDataFromYoutubeForChannels(channel_id):
    channel_id=channel_id

    def getChannelStats(youtube, channel_id):
        request=youtube.channels().list(part='snippet,contentDetails,statistics',id=channel_id)
        response=request.execute()
        return response

    return getChannelStats(youtube, channel_id)

def collectDataFromYoutubeForPlaylist(playlistId):
    playlistId=playlistId

    def getChannelStats(youtube, playlistId):
        request=youtube.playlistItems().list(part='snippet,contentDetails',playlistId=playlistId, maxResults=1000)
        response=request.execute()
        return response

    return getChannelStats(youtube, playlistId)

def collectDataFromYoutubeForVideo(videoId):
    def getChannelStats(youtube, videoId):
        request=youtube.videos().list(part='snippet,contentDetails,statistics',id=videoId,maxResults=1000 )
        response=request.execute()
        response.update
        return response

    return getChannelStats(youtube, videoId)

def collectDataFromYoutubeForVideoComments(videoId):
    def getChannelStats(youtube, videoId):
        comment=youtube.commentThreads().list(part='snippet,replies',videoId=videoId)
        commentresponse=comment.execute()
        return commentresponse

    return getChannelStats(youtube, videoId)