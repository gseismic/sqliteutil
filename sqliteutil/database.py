# coding: utf8
import os
import sqlite3


class Database(object):
    """
    如果文件不存在，自动创建
    """
    def __init__(self, db_file, init=True, check_dir_exist=True):
        self.db_file = db_file
        self._check_dir_exist = check_dir_exist
        self._conn = None
        if init:
            self.init_db()

    def init_db(self):
        if self._check_dir_exist:
            db_dirname = os.path.dirname(os.path.abspath(self.db_file))
            if not os.path.exists(db_dirname):
                print('Making dir: %s ...' % db_dirname)
                os.makedirs(db_dirname)
        self._conn = sqlite3.connect(self.db_file)

    def get_conn(self):
        if not self._conn:
            self.init_db()
        return self._conn

    def close_db(self):
        self._conn.close()
        self._conn = None

    @property
    def conn(self):
        return self.get_conn()
