# -*- coding:utf-8 -*-

import twitter_db
import twitter_handler
import sys

if __name__ == '__main__':
    db_name = 'twitter.db'
    db = twitter_db.twitter_db(db_name)
    twh = twitter_handler.twitter_handler()
    db.create_tables()

    user = 'kokushingo'
    tweets = twh.get_tweets(user, count=10000)
    db.add_tweets(tweets)
