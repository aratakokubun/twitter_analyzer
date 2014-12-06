# -*- coding:utf-8 -*-

from user_crawler_db import user_crawler_db
from twitter_db import twitter_db
from twitter_handler import twitter_handler
import tweepy


class user_crawler():

    def __init__(self, db_init=False):
        self.ucdb = user_crawler_db()
        self.twdb = twitter_db()
        self.twh = twitter_handler()

        if db_init:
            self.ucdb.clear_tables()
            self.twdb.clear_tables()

    def __del__(self):
        pass

    def crawle_users(self, limit=10):
        uncrawled = self.ucdb.get_uncrawled(limit, max_failed=2)
        for item in uncrawled:
            print("crawl user %d" % item[0])
            user_id = item[0]
            self.crawle_user(user_id)

        self.ucdb.commit()
        self.twdb.commit()

    def crawle_user(self, user_id):
        try:
            tweets = self.twh.get_tweets_cursor(user_id)
            self.twdb.add_tweets(tweets)
            self.ucdb.update_crawled(user_id, 1)
        except tweepy.TweepError, e:
            print("Could not get tweets of the user:%s because limit of api or some reasons" % user_id)
            self.ucdb.increment_failed_times(user_id)
            print("error : %s" % e)

    def search_users(self, limit=10):
        unsearched = self.ucdb.get_unsearched(limit, max_failed=1)
        for item in unsearched:
            print("search user %d" % item[0])
            user_id = item[0]
            followers = item[5]
            friends = item[6]
            self.search_user(user_id, followers, friends)

        self.ucdb.commit()
        self.twdb.commit()

    def search_user(self, user_id, followers, friends):
        try:
            # compare followers and friends, and crawl more of them
            if followers > friends:
                # followers
                # followers = self.twh.get_followers(user_id)
                print("search for %d followers of %d" % (followers, user_id))
                followers = self.twh.get_followers_divided_pages(user_id)
                for user in followers:
                    self.ucdb.add_crawl_user(user.id, user.followers_count, user.friends_count)
            else:
                # friends
                # friends = self.twh.get_friends(user_id)
                print("search for %d friends of %d" % (friends, user_id))
                friends = self.twh.get_friends_divided_pages(user_id)
                for user in friends:
                    self.ucdb.add_crawl_user(user.id, user.followers_count, user.friends_count)
            self.ucdb.update_searched(user_id, 1)
        except tweepy.TweepError, e:
            print("Could not get friends of the user:%s because limit of api or some reasons" % user_id)
            self.ucdb.increment_failed_times(user_id)
            print("error : %s" % e)
