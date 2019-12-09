import os
import requests
import sys
import time

class RequestError(Exception):
    pass

class YoutubeDataAPI():
    """
    Class to simplify the use of YouTube Data API V3.
    Using only the API Key.
    """
     
    def __init__(self, APIKey):
        '''
        The APIKey value is the secret token given by google on they administration console.
        You can get one by following the steps given in "".

        The countryCodes variable specifies a list with all the countries what we want to scan.
        '''
        self._apiKey = APIKey
        self._youtubeAPIBaseUrl = "https://www.googleapis.com/youtube/v3"

    def parametersParser(self, parameters):
        '''
        Parameters is a dict()
        '''
        strParameters = ""
        for key, value in parameters.items():
            if value != None:
                if ((strParameters != "") and (key != list(parameters.keys())[0])):
                    strParameters += "&"
                strParameters += key + "=" + value
        
        return strParameters
    
    def sendRequest(self, url):
        '''
        Receives the complete URL to send the requests.

        This function will return the request JSON or will raise a
        RequestError().
        '''
        request = requests.get(url)
        if request.status_code == 429:
            raise RequestError('HTTP Response 429.\n\tYou are making too many requests, please wait and try agin later.')
        return request.json()

    def getVideos(self,
            # Required
            part, #"id,snippet,contentDetails,player,recordingDetails,statistics,status,topicDetails"
            # Filters (You have to specify only one)
            chart = None,
            _id = None,
            myRating = None,
            # Optional request parameters
            maxResults = None,
            onBehalfOfContentOwner = None,
            pageToken = None,
            regionCode = None,
            videoCategoryId = None
            ):
        '''
        This function gets videos from YouTube with the given parameters.
        \n
        Required parameters:
        \t * part
        \n
        Required ONLY ONE of them:
        \t * chart
        \t * _id
        \t * myRating
        \n
        Optional parameters:
        \t * maxResults
        \t * onBehalfOfContentOwner
        \t * pageToken
        \t * regionCode
        \t * videoCategoryId
        '''
        requestParameters = {
            # Required 
            "part": part,
            # Filters
            "chart": chart,
            "id": _id,
            "myRating": myRating,
            # Optional
            "maxResults": maxResults,
            "onBehalfOfContentOwner": onBehalfOfContentOwner,
            "pageToken": pageToken,
            "regionCode": regionCode,
            "videoCategoryId": videoCategoryId
        }

        strParameters = self.parametersParser(requestParameters)

        url = self._youtubeAPIBaseUrl +\
            "/videos?" +\
            strParameters +\
            "&key=" +\
            self._apiKey
        
        try:
            jsonResult = self.sendRequest(url)
        except RequestError:
            jsonResult = None
        
        return jsonResult

    def getChannels(self,
            # Required
            part, #"id,snippet,brandingSettings,contentDetails,invideoPromotion,statistics,topicDetails"
            # Filters (You have to specify only one)
            categoryId = None,
            forUsername = None,
            _id = None,
            managedByMe = None,
            mine = None,
            mySubscribers = None,
            # Optional request parameters
            maxResults = None,
            onBehalfOfContentOwner = None,
            pageToken = None
            ):
        '''
        This function gets all the information from YouTube Channels with the given parameters.
        \n
        Required parameters:
        \t * part
        \n
        Required ONLY ONE of them:
        \t * categoryId
        \t * forUsername
        \t * _id
        \t * managedByMe
        \t * mine
        \t * mySubscribers
        \n
        Optional parameters:
        \t * maxResults
        \t * onBehalfOfContentOwner
        \t * pageToken
        '''
        requestParameters = {
            # Required 
            "part": part,
            # Filters
            "categoryId": categoryId,
            "forUsername": forUsername,
            "_id": _id,
            "managedByMe": managedByMe,
            "mine": mine,
            "mySubscribers": mySubscribers,
            # Optional
            "maxResults": maxResults,
            "onBehalfOfContentOwner": onBehalfOfContentOwner,
            "pageToken": pageToken
        }

        strParameters = self.parametersParser(requestParameters)

        url = self._youtubeAPIBaseUrl +\
            "/channels?" +\
            strParameters +\
            "&key=" +\
            self._apiKey
        
        try:
            jsonResult = self.sendRequest(url)
        except RequestError:
            jsonResult = None
        
        return jsonResult