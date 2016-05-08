#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))

#Variables that contains the user credentials to access Twitter API 
access_token = "4176278119-OkdJTZgtdJh6qZYz483HTdMjcDAVbJQL45CoSgZ"
access_token_secret = "Al6PWC6EXWNPUvvmP964kVDfZivlCyVEEdIUaCQYoecxx"
consumer_key = "j19KmpBmWHDZVbORuUgXj3aMF"
consumer_secret = "MrzG1HCftg9lgEu93QMnjtPfo4AxFb8YkzEXe4HvZZGbFcm4NO"


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        producer.send('test', data)
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['democrat', 'republican'])