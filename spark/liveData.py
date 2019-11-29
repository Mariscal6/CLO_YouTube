from YoutubeDataAPI import YoutubeDataAPI
import socket
import sys
import requests
import requests_oauthlib
import json

def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(tweet_text + '\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)

def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '-72,42,-71,43')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


def processVideos(apikey):
    '''
    This class will be procesing all the information obtained by the YouTube Data API
    and process it to send it to spark.

    The given format will be:

    videoID\tPublished At\tTitle\tDescription\tTags\t
    
    '''
    yt = YoutubeDataAPI(apikey)
    part = "id,statistics,snippet"
    chart = "mostPopular"
    regionCode = "ES"
    youtubeData = json.loads(yt.getVideos(part=part,chart=chart,regionCode=regionCode))

    videos = youtubeData['items']


    return 


if __name__ == "__main__":
    apikey = "AIzaSyDbJzfwtRtzYEn_CC9eNoxFn5JjF7qRx_U"
    TCP_IP = "localhost"
    TCP_PORT = 9009
    conn = None
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((TCP_IP, TCP_PORT))
    s.listen(1)
    print("Waiting for TCP connection...")
    conn, addr = s.accept()
    print("Connected... Starting getting tweets.")
    resp = get_tweets()
    send_tweets_to_spark(resp,conn)
    preprocessVideos(apikey)
    pass