# -*- coding:utf-8 -*-

import sqlite3 as sqlite
from datetime import datetime


class user_crawler_db():

    """
    user crawler table definition
    crawl_users[
        id:integer PRIMARY KEY,
        crawled:integer(boolean)
        friend_searched:integer
        last_update:text
    ]
    """

    db_name = 'user_crawl.db'
    table_name = 'crawl_users'

    def __init__(self):
        self.con = sqlite.connect(self.db_name)
        self.temp_users = []

    def __del__(self):
        self.con.close()

    # -----------------------------------------------------------------------
    # create, clear
    def create_tables(self):
        try:
            self.con.execute("create table if not exists %s(\
                id integer PRIMARY KEY,\
                crawled integer,\
                friend_searched,\
                last_update text)" % self.table_name)
            self.con.commit()
            return True
        except sqlite.Error:
            print('create table failed')
            return False

    def clear_tables(self):
        try:
            self.con.execute("delete from %s" % self.table_name)
            self.con.commit()
        except sqlite.Error:
            print("clear table %s failed" % self.table_name)

    # -----------------------------------------------------------------------
    # add, update
    def add_crawl_user(self, user_id):
        if self.search_crawl_users(user_id):
            return

        try:
            self.con.execute("insert into %s(\
                id,\
                crawled,\
                friend_searched,\
                last_update\
                ) values (\
                %d,\
                %d,\
                %d,\
                '%s'\
                )" % (
                self.table_name,
                user_id,
                0,
                0,
                datetime.now().strftime('%Y/%m/%d')))
            self.con.commit()
        except sqlite.Error:
            print('Error occurred while insert : id=%d' % user_id)

    def update_crawl_users(self, user_id, crawled, friend_searched):
        self.con.execute("update %s \
            set crawled = %d,\
            set friend_searched = %d,\
            set last_update = '%s'\
            where \
            id = %d\
            )" % (
            self.table_name,
            crawled,
            friend_searched,
            datetime.now().strftime('%Y/%m/%d'),
            user_id))
        self.con.commit()

    # -----------------------------------------------------------------------
    # print, get
    def print_crawl_users(self):
        res = self.con.execute("select * from %s" % self.table_name).fetchall()
        for item in res:
            try:
                print(item)
            except UnicodeDecodeError:
                print('decode error')

    def get_crawl_users(self, user_id=None):
        if user_id is None:
            return self.con.execute("select * from %s" % self.table_name).fetchall()
        else:
            return self.con.execute("select * from %s where id=%d" % (self.table_name, user_id)).fetchone()

    def get_uncrawled(self, limit=10):
        return self.con.execute("select * from %s where crawled = 0 order by last_update descend limit %d"
                                % (self.table_name, limit)).fetchall()

    def get_unsearched(self, limit=10):
        return self.con.execute("select * from %s where friend_searched = 0 order by last_update descend limit %d"
                                % (self.table_name, limit)).fetchall()

    # -----------------------------------------------------------------------
    # search
    def search_crawl_users(self, user_id):
        res = self.con.execute("select * from %s where id=%d" % (self.table_name, user_id))
        return True if res else False

