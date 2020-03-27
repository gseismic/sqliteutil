from .database import Database
from .utils import (easy_create_table, easy_create_index)

"""
注意：
    不是长久的解决方案，最次也要用pewee/sqlalchemy
意义:
    基于dict，写入文件
    所有想用文件的地方，用sqlite替代
"""


class Table(object):
    """
    设计原则： 只有commit抛出的异常才catch
    """
    def __init__(self, database_or_db_file, table_name, table_dict, 
                 auto_commit=True, echo=False):
        if isinstance(database_or_db_file, str):
            self._database = Database(db_file=database_or_db_file)
        else:
            self._database = database_or_db_file
        self._table_name = table_name
        self._table_dict = table_dict
        self.auto_commit = auto_commit
        self.echo = echo
        self._table_fields = [item['name'] for item in self.table_dict]
        # 使用SqliteDatabase 因为可以自动维护conn
        # 同时考虑: 因为fetchall需要，所以设计一个SqliteTable只有一个
        # 固定的cursor
        self._cursor = self._database.conn.cursor()

    def create_table(self, create_index=True, only_if_not_exists=True):
        easy_create_table(self.conn, self._cursor, self._table_name, 
                          self.table_dict, 
                          create_index=create_index, 
                          only_if_not_exists=only_if_not_exists,
                          echo=self.echo)

    def create_index(self, only_if_not_exists=True):
        easy_create_index(self.conn, self._cursor, self._table_name, 
                          self.table_dict, 
                          only_if_not_exists=only_if_not_exists,
                          echo=self.echo)

    def execute(self, sql, commit=None):
        self._cursor.execute(sql)
        if commit or self.auto_commit:
            self.commit()

    def executemany(self, sql, list_values, commit=True):
        # default: commit
        self._cursor.executemany(sql, list_values)
        if commit or self.auto_commit:
            self.commit()

    def fetchall(self):
        # 读取数据可能需要以前的游标，而不是重建
        data = self._cursor.fetchall()
        return data

    def update(self, kv, where_stmt, commit=None):
        sql = 'update %s' % self._table_name 
        kv_string = ' and '.join(['%s="%s"' % (str(k), str(v)) \
                                  for k, v in kv.items()])
        sql += ' set ' + kv_string
        if where_stmt:
            sql += ' where ' + where_stmt
        self.execute(sql, commit=commit)

    def select(self, fields, where_stmt, commit=None):
        if fields is None:
            field_string = ','.join(self.self.table_fields)
        else:
            if isinstance(fields, (tuple, list)):
                field_string = ','.join(fields)
            else:
                # string: id, name,
                field_string = fields

        sql = 'select %s' % field_string
        sql += ' from %s' % self._table_name
        sql += ' where %s' % where_stmt
        self.execute(sql, commit=commit)

    # utils
    def insert_dict(self, kv, action='insert into', commit=None):
        if not kv:
            return
        action = self._legalize_insert_action(action)
        # compute sql
        keys = list(kv.keys())
        values = list(kv.values())
        key_string = ','.join([str(key)for key in keys])
        value_string = ','.join([str(v) for v in values])
        sql = '%s %s (%s) values (%s)' % (action, self._table_name, 
                                          key_string, value_string)
        # commit
        self.execute(sql, commit=commit)

    def insert_dict_many(self, list_kv, action='insert into', commit=True):
        # 要求keys一致
        if not list_kv:
            return
        action = self._legalize_insert_action(action)

        list_values = []
        fields = list(list_kv[0].keys())
        for kv in list_kv:
            list_values.append(tuple(kv.values()))
        num_fields = len(fields)

        field_string = ','.join([str(key)for key in fields])
        sql = '%s %s (%s) values (%s)' % (action, self._table_name, 
                                          field_string,
                                          ','.join(['?'] * num_fields))
        self.executemany(sql, list_values, commit=commit)

    def commit(self):
        try:
            self.conn.commit()
        except Exception as e:
            print('Error %s' % str(e))
            self.conn.rollback()

    def close(self):
        self._cursor.close()

    @property
    def cursor(self):
        # 如果conn 已经关闭，可以自动连接
        return self._cursor

    @property
    def conn(self):
        # 如果conn 已经关闭，可以自动连接
        return self._database.conn

    @property
    def database(self):
        return self._database

    def _legalize_insert_action(self, action):
        # check
        action = action.strip().lower()
        if not action.endswith('into'):
            action += ' into'
        assert(action in ['insert into', 'insert or ignore into'
                          'insert or replace into'])
        return action
