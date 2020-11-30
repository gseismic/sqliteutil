# coding: utf8
import os
import time
import sqlite3

"""
ChangeLog:
    Get all tables
"""


class Database(object):
    """
    如果文件不存在，自动创建
    """
    def __init__(self, db_file, init=True, 
                 ensure_dir_exists=True,
                 echo=False, **kwargs):
        self.db_file = db_file
        self.echo = echo
        self._ensure_dir_exists = ensure_dir_exists
        self._conn = None
        self._kwargs = kwargs
        if init:
            self.init()

    def init(self):
        if self._ensure_dir_exists:
            db_dirname = os.path.dirname(os.path.abspath(self.db_file))
            if self.echo:
                print('Database data dir: %s' % db_dirname)
            if not os.path.exists(db_dirname):
                print('Making database dir: %s ...' % db_dirname)
                os.makedirs(db_dirname)
        self._conn = sqlite3.connect(self.db_file, **self._kwargs)

    def execute(self, sql):
        # self.conn.cursor().execute(sql)
        self.conn.execute(sql)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def get_conn(self):
        if not self._conn:
            self.init()
        return self._conn

    def close(self):
        self._conn.close()
        self._conn = None

    @property
    def conn(self):
        return self.get_conn()

    def get_all_tables(self):
        cu = self.conn.cursor()
        sql = 'SELECT Name FROM sqlite_master WHERE type="table"'
        cu.execute(sql)
        table_names = cu.fetchall()
        table_names = [t[0] for t in table_names]
        cu.close()
        return table_names

    def get_table_indices(self, table_name, exclude_autoindex=True):
        # autoindex: 无法涉及primary_key，是无法删除
        sql = ("SELECT name FROM sqlite_master " 
               "WHERE type == 'index' AND "
               "tbl_name == '%s'" % table_name)
        cu = self.conn.cursor()
        cu.execute(sql)
        data = cu.fetchall()
        indices = [it[0] for it in data]
        if exclude_autoindex:
            indices = self._exclude_autoindex(indices)
        return indices

    def _exclude_autoindex(self, indices):
        new_indices = [index for index in indices if index.find('autoindex') ==-1]
        return new_indices

    def get_all_indices(self, exclude_autoindex=True):
        cu = self.conn.cursor()
        sql = 'SELECT Name FROM sqlite_master WHERE type="index"'
        cu.execute(sql)
        r = cu.fetchall()
        indices = [t[0] for t in r]
        cu.close()
        if exclude_autoindex:
            indices = self._exclude_autoindex(indices)
        return indices

    def drop_table(self, table_name):
        if isinstance(table_name, str):
            table_names = [table_name]
        elif isinstance(table_name, (list, tuple)):
            table_names = table_name
        else:
            raise Exception('table_name required to be str/list/tuple')

        for tbl in table_names:
            sql = 'drop table %s' % tbl
            if self.echo:
                print(sql)
            self.execute(sql)

    def drop_index(self, index_or_indices):
        if isinstance(index_or_indices, (list, tuple)):
            indices = index_or_indices
        elif isinstance(index_or_indices, str):
            indices = [index_or_indices]
        else:
            raise Exception('index required to be str/list/tuple')

        # exec
        cu = self.conn.cursor()
        for index in indices:
            sql = 'drop index %s' % index
            if self.echo:
                print(sql)
            cu.execute(sql)

    def drop_all_indices(self, exclude_autoindex=True):
        print('Warning: Drop all indices of all tables ... sleep 10 sec')
        time.sleep(10)
        indices = self.get_all_indices(exclude_autoindex)
        self.drop_index(indices)

    def create_index(self, table_name, field, 
                     option='ASC', ignore_if_exists=False):
        if ignore_if_exists:
            base_sql = 'CREATE INDEX IF NOT EXISTS'
        else:
            base_sql = 'CREATE INDEX'

        # 增加table名
        sql = '%s %s_%s_index on %s (%s ' % (base_sql, table_name, field, table_name, field)
        if option:
            sql += option
        sql += ')'

        self.execute(sql)
