from tweepy import OAuthHandler, API, Stream, StreamListener
from threading import Thread, active_count
from time import time
import json
import gzip
import urllib.request
import os
import pdb
import sys

STATUSES_DIR = 'data/statuses'
IMAGES_DIR = 'data/images'
TWCREDS = {
  "consumer_key": os.getenv("TWCK"),
  "consumer_secret": os.getenv("TWCS"),
  "access_token": os.getenv("TWAT"),
  "token_secret": os.getenv("TWTS")
}

def download(status):
  
  for i, item in enumerate(status.entities['media']):
    t0 = time()
    ext = item['media_url'].split('.')[-1]
    local_path = '%s/%d_%d.%s' % (IMAGES_DIR, status.id, i, ext)
    urllib.request.urlretrieve(item['media_url'], local_path)
    print('%.2lf %s' % (time() - t0, local_path))

  with gzip.open('%s/%d.json.gz' % (STATUSES_DIR, status.id), 'wb') as fp:
    fp.write(json.dumps(status._json).encode())

class MyStreamListener(StreamListener):
	
  def __init__(self, **kwargs):
    self.cnt_tot = len(os.listdir(IMAGES_DIR))
    self.cnt_new = 0
    self.t0 = time()
    super().__init__(kwargs)

  def on_status(self, status):

    if 'media' not in status.entities:
      return
      
    thr = Thread(target=download, args=(status,))
    thr.start()

    self.cnt_new += 1
    self.cnt_tot += len(status.entities['media'])

    time_sec = time() - self.t0
    time_day = time_sec / (24 * 60 * 60)

    print('%d total, %d new, %.3lf per day, %d active threads' % (
      self.cnt_tot, self.cnt_new, self.cnt_new / time_day, active_count()))

if __name__ == "__main__":

  auth = OAuthHandler(TWCREDS['consumer_key'], TWCREDS['consumer_secret'])
  auth.set_access_token(TWCREDS['access_token'], TWCREDS['token_secret'])
  twitter = API(
	auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True,
	retry_count=10, retry_delay=1)

  myStreamListener = MyStreamListener()
  myStream = Stream(auth=twitter.auth, listener=myStreamListener)
  myStream.sample()

