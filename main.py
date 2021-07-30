import json

import tweepy
import pickle
import time
import requests
import en_core_web_sm
import threading

threads = []
buffer = []
organ = []
saved = []
data = []
names = {}
hour = 0
total_tweets = 0


class stream_listener(tweepy.StreamListener):
    def __init__(self):
        super(stream_listener, self).__init__()

    def on_status(self, status):
        global buffer
        global total_tweets
        total_tweets += 1
        if from_creator(status):
            buffer.append(status)


class collecter(threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):
        print("Starting " + self.name)
        global buffer
        global data
        global hour
        while hour < 24:
            buffer = []
            print('starting hour ' + str(hour + 1))
            hour += 1
            try:
                stream.filter(follow=news, is_async=True)
                time.sleep(3600)
                stream.disconnect()
                data += buffer
            except:
                data += buffer


class processor(threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter

    def run(self):
        print("Starting " + self.name)
        global data
        global buffer
        global hour
        global total_tweets
        j = 1
        i = 0
        while hour < 24:
            while i < len(data):
                status = data[i]
                doc = find_entity(status)
                entities = [(X.text, X.label_) for X in doc.ents]
                for e in entities:
                    if e[1] is 'ORG':
                        organ.append(status)
                        if name_check(e[0]):
                            saved.append(status)
                i += 1
            if j <= hour:
                print('length data: ' + str(total_tweets))
                print('length origin: ' + str(len(data)))
                print('length organ: ' + str(len(organ)))
                print('length saved: ' + str(len(saved)))
                j += 1


def name_check(name):
    for key in names.keys():
        if name in names[key]:
            return True
    return False


def from_creator(status):
    if hasattr(status, 'retweeted_status'):
        return False
    elif status.in_reply_to_status_id != None:
        return False
    elif status.in_reply_to_screen_name != None:
        return False
    elif status.in_reply_to_user_id != None:
        return False
    else:
        return True


def resolve_url(url):
    try:
        r = requests.get(url)
    except requests.exceptions.RequestException:
        return (url, None)

    if r.status_code != 200:
        longurl = None
    else:
        longurl = r.url

    return (url, longurl)


def find_entity(status):
    if hasattr(status, 'extended_tweet'):
        doc = nlp(status.extended_tweet['full_text'])
    else:
        doc = nlp(status.text)
    return doc


nlp = en_core_web_sm.load()
with open('config.json', 'r') as f:
    config = json.load(f)

consumer_key = config['consumer_key']
consumer_secret = config['consumer_secret']

access_token = config['access_token']
access_token_secret = config['access_token_secret']

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

with open('names.json','r') as f:
    names =json.load(f)
api = tweepy.API(auth)
try:
    api.verify_credentials()
    print("Authentication OK")
except:
    print("Error during authentication")
# Create API object
api = tweepy.API(auth, wait_on_rate_limit=True,
                 wait_on_rate_limit_notify=True)


days = 7
day_count = 0
while day_count < days:
    threads = []
    buffer = []
    organ = []
    saved = []
    data = []
    names = {}
    hour = 0
    total_tweets = 0
    day_count += 1
    print('starting day ' + str(day_count))
    listener = stream_listener()

    stream = tweepy.Stream(auth=api.auth, listener=listener, tweet_mode='extended')

    news = ['624413', '87818409', '14173315', '428333', '759251', '51241574', '15012486', '28785486', '807095',
            '742143', '5402612', '34713362', '4898091', '15108530', '20402945', '28164923', '1754641', '25053299',
            '91478624', '5988062', '1652541']

    # Create new threads
    thread1 = collecter(1, "Thread-1", 1)
    thread2 = processor(2, "Thread-2", 2)

    # Start new Threads
    thread1.start()
    thread2.start()

    # Add threads to thread list
    threads.append(thread1)
    threads.append(thread2)

    # Wait for all threads to complete
    for t in threads:
        t.join()

    with open('data/day' + str(day_count) + '-data', 'wb') as f:
        pickle.dump(data, f)
    with open('data/day' + str(day_count) + '-organ', 'wb') as f:
        pickle.dump(organ, f)
    with open('data/day' + str(day_count) + '-saved', 'wb') as f:
        pickle.dump(saved, f)
    print('saved day:' + str(day_count) + '\n')
