
from tweepy import Stream, OAuthHandler, StreamListener, TweepError
from kafka import KafkaProducer #Tested on kafka-python 1.0.2
import json
import sys

"""
Can be tested with kafka-console-consumer.sh.
"""

def load_auth_info(keys_file, app_name):

    with open(keys_file) as fp:
        d = json.load(fp)
        for record in d:
            if record['app_name'] == app_name:
                return record

class TwitterStreamListener(StreamListener):

    def on_data(self, data):
        # TODO: remove topic name hardcoding...
        producer.send('test', json.loads(data))        
        return True

if __name__ == '__main__':

    if len(sys.argv) != 3:
        print '>>> Usage: python twifeeds.py twitter_app_keys_file twitter_app_name'
        sys.exit(-1)

    keys = load_auth_info(sys.argv[1], sys.argv[2])

    try:
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'], \
                                 value_serializer=lambda d: json.dumps(d).encode('utf-8'))
        
        auth = OAuthHandler(keys['api_key'], keys['api_secret'])
        auth.set_access_token(keys['access_token'], keys['access_token_secret'])

        stream = Stream(auth = auth, listener = TwitterStreamListener())
        # TODO: Remove filter hardcoding...
        stream.filter(track = ['global warming', 'oil spill'])
        
    except TweepError as e:
        print '>>> tweepy error: ', e
    except KeyboardInterrupt:
        print '>>> Keyboard interrupt received...'
    except Exception as e:
        print '>>> kafka/other errors: ', e
    finally:
        print '>>> Closing twitter stream...'
        stream.disconnect()
        #producer.close()


