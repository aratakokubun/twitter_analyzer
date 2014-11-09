# -*- coding: utf-8 -*-

import sqlite3 as sqlite
import re


class twitter_db:

    """
    twitter table definition
    tweet[
        id:integer PRIMARY KEY,
        user_id:text,
        text:text,
        favorite_count:integer, # TODO is favorited or count?
        retweet_count:integer,
        created:text(Y-m-d H:M:S),
        geo_lat:REAL,
        geo_lng:REAL,
        place:text
    ]
    user[
        id:integer PRIMARY KEY,
        screen_name:text,
        name:text,
        statuses_count:integer, # count of tweets
        followers_count:integer,
        friends_count:integer,
        verified:integer(boolean),
        protected:integer(boolean),
        location:text
    ]
    """

    def __init__(self, db_name):
        self.con = sqlite.connect(db_name)
        self.temp_users = []

    def __del__(self):
        self.con.close()

    # -----------------------------------------------------------------------
    # print
    def print_tweets(self):
        res = self.con.execute("select * from tweets order by id limit 200").fetchall()
        for item in res:
            try:
                print(item)
            except UnicodeEncodeError:
                print('decode error')

    def count_tweets(self, user_id=None):
        if user_id is None:
            res = self.con.execute("select count(*) as cnt from tweets order by id")
        else:
            res = self.con.execute("select count(*) as cnt from tweets order by id where user_id='%s'" % (user_id))
        return res.fetchone()[0]

    def get_tweets(self, user_id=None, limit=10000):
        return self.con.execute("select * from tweets order by id limit %d" % (limit)).fetchall()

    def print_users(self):
        res = self.con.execute("select * from users order by id").fetchall()
        for item in res:
            try:
                print(item)
            except UnicodeEncodeError:
                print('decode error')

    def count_uers(self):
        res = self.con.execute("select count(*) as cnt from users order by id")
        return res.fetchone()[0]

    def get_users(self, limit=10000):
        return self.con.execute("select * from users order by id limit %d" % (limit)).fetchall()

    # -----------------------------------------------------------------------
    # search
    def search_tweets(self, tweet_id):
        res = self.con.execute("select * from tweets where id='%s'" % (tweet_id)).fetchone()
        return True if res else False

    def search_users(self, user_id):
        res = self.con.execute("select * from users where id='%s'" % (user_id)).fetchone()
        return True if res else False

    # -----------------------------------------------------------------------
    # add
    def add_tweets(self, tweets):
        for tweet in tweets:
            user = tweet.user
            self.add_user(user)
            if user.geo_enabled:
                self.add_tweet(tweet, tweet.geo.lat, tweet.geo.lng)
            else:
                self.add_tweet(tweet, 0.0, 0.0)

    def add_tweet(self, tweet, lat, lng):
        if self.search_tweets(tweet.id):
            return

        try:
            self.con.execute("insert into tweets(\
                id,\
                user_id,\
                text,\
                favorite_count,\
                retweet_count,\
                created,\
                geo_lat,\
                geo_lng\
                ) values (\
                %d,\
                %d,\
                '%s',\
                %d,\
                %d,\
                '%s',\
                %f,\
                %f\
                )" % (\
                tweet.id,\
                tweet.user.id,\
                tweet.text,\
                tweet.favorite_count,\
                tweet.retweet_count,\
                tweet.created_at,\
                lat,\
                lng))
            self.con.commit()
        except:
            print('Error occurred while insert : id=%d' % (tweet.id))

    def add_user(self, user):
        # user already added to list in this time
        if user.id in self.temp_users:
            return

        self.temp_users.append(user.id)
        if self.search_users(user.id):
            self.update_user(user)
            return

        try:
            self.con.execute("insert into users(\
                id,\
                screen_name,\
                name,\
                statuses_count,\
                followers_count,\
                friends_count,\
                verified,\
                protected) \
                values (\
                %d,\
                '%s',\
                '%s',\
                %d,\
                %d,\
                %d,\
                %d,\
                %d\
                )" % (\
                user.id,\
                user.screen_name,\
                user.name,\
                user.statuses_count,\
                user.followers_count,\
                user.friends_count,\
                user.verified,\
                user.protected))
            self.con.commit()
        except:
            print('Error occurred while insert : user_id:%d' % (user.id))

    # -----------------------------------------------------------------------
    # udpate
    def update_user(self, user):
        self.con.execute("update users \
            set screen_name = '%s',\
            set name = '%s',\
            set statuses_count = %d,\
            set followers_count = %d,\
            set friends_count = %d,\
            set verified = %d,\
            set protected = %f\
            where \
            id = %d\
            )" % (\
            user.screen_name,\
            user.name,\
            user.statuses_count,\
            user.followers_count,\
            user.friends_count,\
            user.verified,\
            user.protected,\
            user.id))
        self.con.commit()

    # -----------------------------------------------------------------------
    def arrange_str(self, str):
        sqlitter = re.compile('\\W*')
        return "".join([s.lower() for s in sqlitter.split(str) if s!=''])

    # -----------------------------------------------------------------------
    def create_tables(self):
        try :
            self.con.execute("create table tweets(\
                id integer PRIMARY KEY,\
                user_id integer,\
                text text,\
                favorite_count integer,\
                retweet_count integer,\
                created text,\
                geo_lat REAL,\
                geo_lng REAL)")
            self.con.execute("create table users(\
                id integer PRIMARY KEY,\
                screen_name text,\
                name text,\
                statuses_count integer,\
                followers_count integer,\
                friends_count integer,\
                verified integer,\
                protected integer)")
            self.con.commit()
        except UnicodeDecodeError:
            print('create table failed')

    def clear_tables(self):
        self.con.execute("delete from tweets")
        self.con.execute("delete from users")
        self.con.commit()
