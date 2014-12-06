# -*- coding:utf-8 -*-

import sqlite3 as sqlite
from datetime import datetime


class user_crawler_db():

    """
    user crawler table definition
    crawl_users[
        [0]id:integer PRIMARY KEY,
        [1]crawled:integer(boolean),
        [2]friend_searched:integer(boolean),
        [3]failed:integer,
        [4]last_update:text,
        [5]followers_count:integer,
        [6]friends_count:integer
    ]
    """

    db_name = 'user_crawl.db'
    table_name = 'crawl_users'

    def __init__(self):
        self.con = sqlite.connect(self.db_name)
        self.temp_users = []
        self.create_tables()

    def __del__(self):
        self.con.close()

    # -----------------------------------------------------------------------
    # create, clear
    def create_tables(self):
        try:
            self.con.execute("\
                    create table if not exists %s(\
                    id integer PRIMARY KEY,\
                    crawled integer,\
                    friend_searched,\
                    failed,\
                    last_update text,\
                    followers_count integer,\
                    friends_count integer)" % self.table_name)
            self.con.commit()
            return True
        except sqlite.Error, e:
            print('create table failed, error : %s' % e)
            return False

    def clear_tables(self):
        try:
            self.con.execute("delete from %s" % self.table_name)
            self.con.commit()
        except sqlite.Error, e:
            print("clear table %s failed, error : %s" % (self.table_name, e))

    # -----------------------------------------------------------------------
    # add, update
    def add_crawl_user(self, user_id, followers_count=0, friends_count=0):
        if self.search_crawl_users(user_id):
            self.update_user(user_id, followers_count, friends_count)
            return

        try:
            self.con.execute("\
                insert into %s(\
                    id,\
                    crawled,\
                    friend_searched,\
                    failed,\
                    last_update,\
                    followers_count,\
                    friends_count\
                ) values (\
                    %d,\
                    %d,\
                    %d,\
                    %d,\
                    '%s',\
                    %d,\
                    %d\
                )" % (
                self.table_name,
                user_id,
                0,
                0,
                0,
                datetime.now().strftime('%Y/%m/%d'),
                followers_count,
                friends_count))
        except sqlite.Error, e:
            print('Error occurred while insert : id=%d, error : %s' % (user_id, e))

    def update_user(self, user_id, followers_count, friends_count):
        self.con.execute("\
            update %s\
            set followers_count = %d, friends_count = %d, last_update = '%s'\
            where id = %d"
                         % (self.table_name, followers_count, friends_count, datetime.now().strftime('%Y/%m/%d'), user_id))

    def update_crawl_users(self, user_id, crawled, friend_searched):
        self.con.execute("\
            update %s \
            set crawled = %d, friend_searched = %d, last_update = '%s'\
            where id = %d"
                         % (self.table_name, crawled, friend_searched, datetime.now().strftime('%Y/%m/%d'), user_id))

    def update_crawled(self, user_id, crawled):
        self.con.execute("\
            update %s\
            set crawled = %d, last_update = '%s'\
            where id = %d"
                         % (self.table_name, crawled, datetime.now().strftime('%Y/%m/%d'), user_id))

    def update_searched(self, user_id, friend_searched):
        self.con.execute("\
            update %s\
            set friend_searched = %d, last_update = '%s'\
            where id = %d"
                         % (self.table_name, friend_searched, datetime.now().strftime('%Y/%m/%d'), user_id))

    def increment_failed_times(self, user_id):
        self.con.execute("\
            update %s\
            set failed = failed + 1\
            where id = %d"
                         % (self.table_name, user_id))

    def reset_failed_times(self, user_id):
        self.con.execute("\
            update %s\
            set failed = 0\
            where id = %d"
                         % (self.table_name, user_id))

    # -----------------------------------------------------------------------
    # get
    def get_crawl_users(self, user_id=None):
        if user_id is None:
            return self.con.execute("select * from %s" % self.table_name).fetchall()
        else:
            return self.con.execute("select * from %s where id=%d" % (self.table_name, user_id)).fetchone()

    def get_uncrawled(self, limit=10, max_failed=10):
        return self.con.execute("\
            select * from %s where crawled = 0 and failed < %d order by last_update desc limit %d"
                                % (self.table_name, max_failed, limit)).fetchall()

    def get_unsearched(self, limit=10, max_failed=10):
        return self.con.execute("\
            select * from %s where friend_searched = 0 and failed < %d order by last_update desc limit %d"
                                % (self.table_name, max_failed, limit)).fetchall()

    def get_searched(self):
        return self.con.execute("\
            select * from %s where friend_searched = 1"
                                % self.table_name).fetchall()

    # -----------------------------------------------------------------------
    # search
    def search_crawl_users(self, user_id):
        res = self.con.execute("select * from %s where id=%d" % (self.table_name, user_id)).fetchone()
        return True if res else False

    def commit(self):
        self.con.commit()

