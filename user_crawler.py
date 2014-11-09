# -*- coding:utf-8 -*-

import sqlite3 as sqlite
from datetime import datetime


class user_crawler():

    """
    user crawler table definition
    crawl_users[
        id:integer PRIMARY KEY,
        crawled:integer(boolean)
        friend_searched:integer
        last_update:text
    ]
    """

    table_name = 'crawl_users'

    def __init__(self, db_name):
        self.con = sqlite.connect(db_name)
        self.temp_users = []

    def __del__(self):
        self.con.close()

    # -----------------------------------------------------------------------
    # print, get
    def print_crawl_users(self):
        res = self.con.execute("select * from %s" % self.table_name).fetchall()
        for item in res:
            try:
                print(item)
            except UnicodeDecodeError:
                print('decode error')

    def get_crawl_users(self, user_id = None):
        if user_id is None:
            return self.con.execute("select * from %s" % self.table_name).fetchall()
        else:
            return self.con.execute("select * from %s where id=%d" % (self.table_name, user_id)).fetchone()

    def get_uncrawled(self):
        return self.con.execute("select * from %s where crawled = 0 order by last_update" % self.table_name).fetchall()

    def get_unsearched(self):
        return self.con.execute("select * from %s where friend_searched = 0 order by last_update" % self.table_name).fetchall()

    # -----------------------------------------------------------------------
    # search
    def search_crawl_users(self, user_id):
        res = self.con.execute("select * from %s where id=%d" % (self.table_name, user_id))
        return True if res else False

    # -----------------------------------------------------------------------
    # add, update
    def add_crawl_user(self, user):
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
                user.id,
                0,
                0,
                datetime.now().strftime('%Y/%m/%d')))
            self.con.commit()
        except UnicodeDecodeError:
            print('Error occurred while insert : id=%d' % user.id)

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
    # create, clear
    def create_tables(self):
        try:
            self.con.execute("create table if not exists %s(\
                id integer PRIMARY KEY,\
                crawled integer,\
                friend_searched,\
                last_update text)" % self.table_name)
            self.con.commit()
        except UnicodeDecodeError:
            print('create table failed')

    def clear_tables(self):
        self.con.execute("delete from %s" % self.table_name)
        self.con.commit()
