# -*- coding:utf-8 -*-

import twitter_db
import twitter_handler
import statics_plot
import user_crawler_db


if __name__ == '__main__':
    db_name = 'twitter.db'
    db = twitter_db.twitter_db(db_name)
    twh = twitter_handler.twitter_handler()
    stp = statics_plot.statics_plot()
    usc = user_crawler_db.user_crawler_db('twitter_user_crawler.db')

    """
    db.create_tables()
    """

    """
    user = 'kokushingo'
    tweets = twh.get_tweets(user, count=10000)
    db.add_tweets(tweets)
    """

    """
    items = db.get_tweets()
    time_items = {}
    new = datetime.now() - relativedelta(years=10)
    old = datetime.now()
    for item in items:
        created_at = datetime.strptime(item[5], '%Y-%m-%d %H:%M:%S')
        if created_at in time_items:
            time_items[created_at] += 1
        else:
            time_items[created_at] = 1

        if new < created_at:
            new = created_at
        if old > created_at:
            old = created_at

    url = stp.generate_statics_plotly(time_items, old, new, days=len(time_items)/10, file='temp')
    """

    usc.create_tables()
    users = db.get_users(limit=1)
    friends = twh.get_friends(users[0][0])
    for i in friends:
        usc.add_crawl_user(i)

    
