import math
from .database import Database
from ._helpers import (easy_create_table, easy_create_index)

"""
注意：
    不是长久的解决方案，最次也要用pewee/sqlalchemy: pewee 等不适合高速插入，用于读取可以
意义:
    基于dict，写入文件
    所有想用文件的地方，用sqlite替代
ChangeLog:
    2020-04-16 23:43:51 chunk_size: default 5000
"""


class Table(object):
    """
    设计原则： 只有commit抛出的异常才catch
    """
    def __init__(self, database_or_db_file, table_name, table_fields, 
                 auto_commit=False, echo=False, db_options={}):
        if isinstance(database_or_db_file, str):
            # XXX 需要手动关闭吗？ 这里不推荐使用
            # assert(0, '可能需要需要手动关闭, 不建议使用')
            raise Exception('可能需要需要手动关闭, 不建议使用')
            self._database = Database(db_file=database_or_db_file,
                                      **db_options)
        else:
            self._database = database_or_db_file
        self._table_name = table_name
        self._table_fields = table_fields
        self.auto_commit = auto_commit
        self.echo = echo
        self._fields = [item['name'] for item in self._table_fields]
        # 使用SqliteDatabase 因为可以自动维护conn
        # 同时考虑: 因为fetchall需要，所以设计一个SqliteTable只有一个
        # 固定的cursor
        self._cursor = self._database.conn.cursor()

    def create_table(self, ignore_if_exists=False):
        # ignore_if_exists: False 时，如果表存在，raise
        self.create(create_index=False, 
                    ignore_if_exists=ignore_if_exists)

    def create(self, create_index=False, ignore_if_exists=False):
        # create or raise
        easy_create_table(self.conn, self._cursor, self._table_name, 
                          self._table_fields, 
                          create_index=create_index, 
                          ignore_if_exists=ignore_if_exists,
                          echo=self.echo)

    def create_index(self, ignore_if_exists=False):
        easy_create_index(self.conn, self._cursor, self._table_name, 
                          self._table_fields, 
                          ignore_if_exists=ignore_if_exists,
                          echo=self.echo)

    def execute(self, sql, commit=None):
        if self.echo:
            print('SQL: %s' % sql)
        self._cursor.execute(sql)
        # print('res', n)
        if commit or self.auto_commit:
            self.commit()

    def executemany(self, sql, list_values, commit=True, chunk_size=50000):
        # default: commit
        if self.echo:
            print('SQL: %s' % sql)

        if not list_values:
            return

        if chunk_size < 1:
            chunk_size = 1

        # 2020-04-17 01:08:17 fixed 重复了
        # self._cursor.executemany(sql, list_values)
        n = len(list_values)
        n_chunks = math.ceil(n/float(chunk_size))
        # print(n)
        for i in range(n_chunks):
            # print('write chunk_size: %d/%d ..' % (i+1, n_chunks))
            # print(i*chunk_size, min((i+1)*chunk_size, n))
            self._cursor.executemany(
                sql, list_values[i*chunk_size: min((i+1)*chunk_size, n)])

        if commit or self.auto_commit:
            self.commit()

    def fetchall(self):
        # 读取数据可能需要以前的游标，而不是重建
        data = self._cursor.fetchall()
        return data

    def fetchone(self):
        # 读取数据可能需要以前的游标，而不是重建
        data = self._cursor.fetchone()
        return data

    def dict_update(self, kv, where, commit=None):
        sql = 'update %s' % self._table_name 
        # fixed:  and --> ','
        kv_string = ','.join(['%s="%s"' % (str(k), str(v)) \
                                  for k, v in kv.items()])
        sql += ' set ' + kv_string
        if where:
            sql += ' where ' + where
        self.execute(sql, commit=commit)

    def dict_select(self, fields, where, option=None):
        if fields is None:
            fields = self._fields
        elif isinstance(fields, (tuple, list)):
            pass
        elif isinstance(fields, str):
            # string: id, name,
            fields = [f.strip() for f in fields.split(',')]
        else:
            assert('Bad field')
        field_string = ','.join(fields)

        sql = 'select %s' % field_string
        sql += ' from %s' % self._table_name
        if where:
            sql += ' where %s' % where
        if option:
            sql += ' %s' % option
        self.execute(sql)
        data = self.fetchall()
        rv = [dict(zip(fields, tu)) for tu in data]
        return rv

    # utils
    def dict_insert(self, kv, action='insert into', commit=None):
        if not kv:
            return
        action = self._legalize_insert_action(action)
        # compute sql
        keys = list(kv.keys())
        values = list(kv.values())
        key_string = ','.join([str(key)for key in keys])
        value_string = ','.join(['"%s"' % str(v) for v in values])
        sql = '%s %s (%s) values (%s)' % (action, self._table_name, 
                                          key_string, value_string)
        # commit
        self.execute(sql, commit=commit)

    def dict_insertmany(self, list_kv, action='insert into', 
                        chunk_size=50000,
                        commit=True):
        # 要求keys一致: 但并不检验
        if not list_kv:
            return
        action = self._legalize_insert_action(action)

        list_values = []
        fields = list(list_kv[0].keys())
        n_fields = len(fields)
        for kv in list_kv:
            list_values.append(tuple(kv.values()))

        field_string = ','.join([str(key)for key in fields])
        sql = '%s %s (%s) values (%s)' % (action, self._table_name, 
                                          field_string,
                                          ','.join(['?'] * n_fields))
        self.executemany(sql, list_values, chunk_size=chunk_size, commit=commit)

    def list_insertmany(self, list_values, action='insert into', 
                        chunk_size=50000, commit=True):
        n_fields = len(self._fields)
        action = self._legalize_insert_action(action)
        sql = '%s %s values (%s)' % (action, self._table_name, 
                                     ','.join(['?'] * n_fields))
        self.executemany(sql, list_values, chunk_size=chunk_size, commit=commit)

    def delete(self, fields, where, commit=None):
        sql = 'delete ' 
        sql += ' from %s' % self._table_name
        sql += ' where %s' % where
        self.execute(sql, commit=commit)

    def delete_all(self, commit=None):
        sql = 'delete from %s' % self._table_name
        self.execute(sql, commit=commit)

    def get_all_index(self):
        # 获取当前已经被创建的index
        sql = ("SELECT name FROM sqlite_master " 
               "WHERE type == 'index' AND "
               "tbl_name == '%s'" % self._table_name)
        self.execute(sql)
        data = self.fetchall()
        return [it[0] for it in data]

    def drop_all_index(self):
        # https://stackoverflow.com/questions/2121583/how-to-drop-all-indexes-of-a-sqlite-table
        list_index = self.get_all_index()
        for index in list_index:
            print('drop index %s ...' % index)
            self.drop_index(index)

    def drop_index(self, index):
        sql = 'drop index %s' % index
        self.execute(sql, commit=True)

    def drop_table(self):
        sql = 'drop table if exists %s' % self._table_name
        self.execute(sql, commit=True)

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
        assert(action in ['insert into', 'insert or ignore into',
                          'insert or replace into'])
        return action
