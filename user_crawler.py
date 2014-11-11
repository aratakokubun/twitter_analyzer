# -*- coding:utf-8 -*-

from user_crawler_db import user_crawler_db as ucdb
from twitter_db import twitter_db as twdb
from twitter_handler import twitter_handler
import tweepy


class user_crawler():

    def __init__(self):
        self.cdb = ucdb()
        self.tdb = twdb()
        self.twh = twitter_handler()

    def __del__(self):
        pass

    def crawle_users(self):
        uncrawled = self.cdb.get_uncrawled()
        for item in uncrawled:
            user_id = item[0]
            searched = item[2]
            self.crawle_user(user_id, searched)

    def crawle_user(self, user_id, searched):
        try:
            tweets = self.twh.get_tweets_cursor(user_id)
            self.tdb.add_tweets(tweets)
            self.cdb.update_crawl_users(user_id, 1, searched)
        except tweepy.TweepError:
            print("Could not get tweets of the user:%s because limit of api or some reasons" % user_id)

    def search_users(self):
        unsearched = self.cdb.get_unsearched()
        for item in unsearched:
            user_id = item[0]
            crawled = item[1]
            self.search_user(user_id, crawled)

    def search_user(self, user_id, crawled):
        try:
            friends = self.twh.get_friends(user_id)
            for user in friends:
                self.cdb.add_crawl_user(user.id)
            self.cdb.update_crawl_users(user_id, crawled, 1)
        except tweepy.TweepError:
            print("Could not get friends of the user:%s because limit of api or some reasons" % user_id)

