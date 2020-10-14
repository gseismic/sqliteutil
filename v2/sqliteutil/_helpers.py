# coding: utf8
"""
table_dict = [
    {'name': 'name', 'dtype': 'text', 'index': False, 'unique': False, 'nullable': True}
]

ChangeLog
    2020-04-20 16:30:41 ignore_if_exists -> default=False
    2020-04-20 16:36:36 删除了不必要的函数

2020-03-28 01:58:46 from sqliteutil_v2.py
"""


def commit_or_rollback_raise(conn):
    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def get_createtable_sql(table_name, table_dict, ignore_if_exists):
    if ignore_if_exists:
        sql = 'CREATE TABLE IF NOT EXISTS %s(' % table_name
    else:
        sql = 'CREATE TABLE %s(' % table_name

    primary_keys = []
    for item in table_dict:
        # print('item', item)
        sql += '%s %s' % (item['name'], item['dtype'])
        nullable = item.get('nullable', True)
        if not nullable:
            sql += ' NOT NULL'

        unique = item.get('unique', False)
        if unique:
            sql += ' UNIQUE'

        default = item.get('default')
        if default:
            sql += ' DEFAULT ' + str(default)

        primary = item.get('primary', False)
        if primary:
            primary_keys.append(item['name'])

        sql += ','

    if primary_keys:
        sql += 'PRIMARY KEY(' + ','.join(primary_keys) + ')'
    else:
        sql = sql.rstrip(',')
    sql += ')'
    return sql


def get_index_fields(table_name, table_dict):
    index_fields = []
    for item in table_dict:
        if item.get('index', False):
            index_fields.append(item['name'])
    return index_fields


def safe_execute_commit_sql(conn, cur, sql, echo=False):
    if echo:
        print('SQL: %s' % sql)
    try:
        cur.execute(sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def easy_create_table(conn, cur, table_name, table_dict, create_index=True,
                 ignore_if_exists=False, echo=False):
    sql = get_createtable_sql(table_name, table_dict, ignore_if_exists)
    safe_execute_commit_sql(conn, cur, sql, echo=echo)
    if create_index:
        easy_create_index(conn, cur, table_name, table_dict,
                          ignore_if_exists, echo)


def easy_create_index(conn, cur, table_name, table_dict, 
                      ignore_if_exists=False, echo=False):
    index_fields = get_index_fields(table_name, table_dict)
    for field in index_fields:
        try_create_index(conn, cur, table_name, field,
                         ignore_if_exists=ignore_if_exists,
                         echo=echo)


def try_create_index(conn, cur, table_name, field, option='ASC',
                 ignore_if_exists=True, echo=False):
    # print('Try creating index ...')
    if ignore_if_exists:
        base_sql = 'CREATE INDEX IF NOT EXISTS'
    else:
        base_sql = 'CREATE INDEX'

    # 增加table名
    sql = '%s %s_%s_index on %s (%s ' % (base_sql, table_name, field, table_name, field)
    if option:
        sql += option

    sql += ')'
    # print(sql)
    safe_execute_commit_sql(conn, cur, sql, echo=echo)
