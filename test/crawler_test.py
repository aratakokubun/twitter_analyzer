# -*- coding:utf-8 -*-

import unittest
from user_crawler_db import user_crawler_db
import sqlite3


class crawler_test(unittest.TestCase):

    def setUp(self):
        self.uc = user_crawler_db()

    def test_crawler_db(self):
        self.uc.clear_tables()
        self.assertTrue(self.uc.create_tables(), "Create table failed")
        self.assertIsNotNone(self.uc.get_crawl_users(), "Table is not Empty")

        user_id = 'no integer type id'
        self.assertRaises(TypeError, self.uc.add_crawl_user, user_id)
        user_id = 0
        try:
            self.uc.add_crawl_user(user_id)
            raised = True
        except sqlite3.Error:
            raised = False
        self.assertTrue(raised, "Add user error")
        self.assertIsNotNone(self.uc.get_crawl_users(), "Table is empty")

if __name__ == "__main__":
    unittest.main()