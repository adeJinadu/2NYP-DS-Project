# -*- coding: utf-8 -*-
"""
Created on Tue Jun 19 11:18:37 2018
Updated on Sunday October 25,2020.

@author: Fesh
"""
###################################################################################
##1. This Script will capture and save to file, a stream of tweets according to the keyworkd input by the user.
##2. The file is saved as a "keyword.json"
##3. If this script is run more than once with the same keyword, the initial file saved with the keyword will be incremented
##4. Script will run till it is stopped with "Ctrl+C" twice.
##################################################################################

import tweepy, datetime
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json, time
from http.client import IncompleteRead

access_token = OAUTH_TOKEN = "51103332-f0u3XRJ2mRP2LI0j6EckV3wBKlIQm43BvTAavoVRX"
access_token_secret = OAUTH_TOKEN_SECRET = "XfF8l5BnYGquqLxnHCZ3jBBBZo91RFLoyKqqBEogWDmG6"
consumer_key = APP_KEY = "YSbe5TBgDGOGUrFUnumXyYPZa"
consumer_secret = APP_SECRET = "b5vflcv8sTS8K9Aaj3Jt2kvHPWfLYAI81998aASYr6NVx3pIiJ"

auth =  tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token,access_token_secret)

api=tweepy.API(auth)
#api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
topic=input('What are we tracking on Twitter?: ')

class MyStreamListener(tweepy.StreamListener):
    def on_data(self, data, track=topic):
        with open(topic + ".json", "a",encoding="utf-8") as file:
            file.write(data)
            print('Streaming....')
    def on_error(self, status_code):
        if status_code == 420:
            return False

    def on_exception(self, exception):
        """Called when an unhandled exception occurs."""
        print('S','\nO','\nM','\nE','\nT','\nH','\nI','\nN','\nG','\n\nF','\nA','\nI','\nL','\nE','\nD', '\nSleeping 5 seconds')
        time.sleep(5)
        return True


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

myStream.filter(track=[topic])