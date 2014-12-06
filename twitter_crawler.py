# -*- coding:utf-8 -*-

from user_crawler import user_crawler


if __name__ == '__main__':
    usc = user_crawler()

    usc.search_users(limit=1)
    usc.crawle_users(limit=50)