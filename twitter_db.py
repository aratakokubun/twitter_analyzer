# -*- coding: utf-8 -*-

import sqlite3 as sqlite
import re


class twitter_db:

    """
    twitter table definition
    tweets[
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
    users[
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

    db_name = 'twitter.db'
    tweet_table = 'tweets'
    user_table = 'users'

    def __init__(self):
        self.con = sqlite.connect(self.db_name)
        self.temp_users = []
        self.create_tables()

    def __del__(self):
        self.con.close()

    # -----------------------------------------------------------------------
    def create_tables(self):
        try:
            self.con.execute("\
                create table if not exists %s(\
                id integer PRIMARY KEY,\
                user_id integer,\
                text text,\
                favorite_count integer,\
                retweet_count integer,\
                created text,\
                geo_lat REAL,\
                geo_lng REAL)" % self.tweet_table)
            self.con.execute("\
                create table if not exists %s(\
                id integer PRIMARY KEY,\
                screen_name text,\
                name text,\
                statuses_count integer,\
                followers_count integer,\
                friends_count integer,\
                verified integer,\
                protected integer)" % self.user_table)
            self.con.commit()
        except sqlite.Error, e:
            print('create table failed, error %s' % e)

    def clear_tables(self):
        self.con.execute("delete from %s" % self.tweet_table)
        self.con.execute("delete from %s" % self.user_table)
        self.con.commit()

    # -----------------------------------------------------------------------
    # add
    def add_tweets(self, tweets):
        for tweet in tweets:
            user = tweet.user
            self.add_user(user)
            if user.geo_enabled and tweet.geo is not None:

                self.add_tweet(tweet, tweet.geo['coordinates'][1], tweet.geo['coordinates'][0])
            else:
                self.add_tweet(tweet, 0.0, 0.0)

        self.commit()

    def add_tweet(self, tweet, lat, lng):
        if self.search_tweets(tweet.id):
            return

        try:
            self.con.execute("\
                insert into %s(\
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
                )" % (
                self.tweet_table,
                tweet.id,
                tweet.user.id,
                tweet.text,
                tweet.favorite_count,
                tweet.retweet_count,
                tweet.created_at,
                lat,
                lng))
        except sqlite.Error, e:
            print('Error occurred while insert : id=%d, error : %s' % (tweet.id, e))

    def add_user(self, user):
        # user already added to list in this time
        if user.id in self.temp_users:
            return

        self.temp_users.append(user.id)
        if self.search_users(user.id):
            self.update_user(user)
            return

        try:
            self.con.execute("\
                insert into %s(\
                    id,\
                    screen_name,\
                    name,\
                    statuses_count,\
                    followers_count,\
                    friends_count,\
                    verified,\
                    protected\
                ) values (\
                    %d,\
                    '%s',\
                    '%s',\
                    %d,\
                    %d,\
                    %d,\
                    %d,\
                    %d\
                )" % (
                self.user_table,
                user.id,
                user.screen_name,
                user.name,
                user.statuses_count,
                user.followers_count,
                user.friends_count,
                user.verified,
                user.protected))
        except sqlite.Error, e:
            print('Error occurred while insert : user_id:%d, error : %s' % (user.id, e))

    # -----------------------------------------------------------------------
    # udpate
    def update_user(self, user):
        self.con.execute("\
            update %s \
            set screen_name='%s', name='%s', statuses_count=%d, followers_count=%d, friends_count=%d, verified=%d, protected=%d \
            where id = %d"
                         % (self.user_table, user.screen_name, user.name, user.statuses_count, user.followers_count, user.friends_count, user.verified, user.protected, user.id))

    # -----------------------------------------------------------------------
    # print
    def print_tweets(self):
        res = self.con.execute("select * from %s order by id limit 200" % self.tweet_table).fetchall()
        for item in res:
            try:
                print(item)
            except UnicodeEncodeError:
                print('decode error')

    def count_tweets(self, user_id=None):
        if user_id is None:
            res = self.con.execute("select count(*) as cnt from %s order by id" % self.tweet_table)
        else:
            res = self.con.execute("select count(*) as cnt from %s order by id where user_id='%s'"
                                   % (self.tweet_table, user_id))
        return res.fetchone()[0]

    def get_tweets(self, user_id=None, limit=10000):
        if user_id is None:
            return self.con.execute("select * from %s order by id limit %d" % (self.tweet_table, limit)).fetchall()
        else:
            return self.con.execute("select * from %s where user_id=%d order by id limit %d"
                                    % (self.tweet_table, user_id, limit)).fetchall()

    def print_users(self):
        res = self.con.execute("select * from %s order by id" % self.user_table).fetchall()
        for item in res:
            try:
                print(item)
            except UnicodeEncodeError:
                print('decode error')

    def count_users(self):
        res = self.con.execute("select count(*) as cnt from %s order by id" % self.user_table)
        return res.fetchone()[0]

    def get_users(self, limit=10000):
        return self.con.execute("select * from %s order by id limit %d" % (self.user_table, limit)).fetchall()

    def get_user(self, user_id):
        return self.con.execute("select * from %s where id=%d" % (self.user_table, user_id)).fetchone()

    # -----------------------------------------------------------------------
    # search
    def search_tweets(self, tweet_id):
        res = self.con.execute("select * from %s where id='%s'" % (self.tweet_table, tweet_id)).fetchone()
        return True if res else False

    def search_users(self, user_id):
        res = self.con.execute("select * from %s where id='%s'" % (self.user_table, user_id)).fetchone()
        return True if res else False

    def commit(self):
        self.con.commit()
