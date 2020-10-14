# coding: utf8
"""
table_dict = [
    {'name': 'name', 'dtype': 'text', 'index': False, 'unique': False, 'nullable': True}
]

2020-03-28 01:58:46 from sqliteutil_v2.py
"""


def commit_or_rollback_raise(conn):
    try:
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e


def get_createtable_sql(table_name, table_dict, only_if_not_exists):
    if only_if_not_exists:
        sql = 'CREATE TABLE IF NOT EXISTS %s(' % table_name
    else:
        sql = 'CREATE TABLE %s(' % table_name

    primary_keys = []
    for item in table_dict:
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
                 only_if_not_exists=True, echo=False):
    sql = get_createtable_sql(table_name, table_dict, only_if_not_exists)
    safe_execute_commit_sql(conn, cur, sql, echo=echo)
    if create_index:
        easy_create_index(conn, cur, table_name, table_dict,
                          only_if_not_exists, echo)


def easy_create_index(conn, cur, table_name, table_dict, 
                      only_if_not_exists=True, echo=False):
    index_fields = get_index_fields(table_name, table_dict)
    for field in index_fields:
        try_create_index(conn, cur, table_name, field,
                         only_if_not_exists=only_if_not_exists,
                         echo=echo)


def try_create_index(conn, cur, table_name, field, option='ASC',
                 only_if_not_exists=True, echo=False):
    # print('Try creating index ...')
    if only_if_not_exists:
        base_sql = 'CREATE INDEX IF NOT EXISTS'
    else:
        base_sql = 'CREATE INDEX'

    # 增加table名
    sql = '%s %s_%s_index on %s (%s ' % (base_sql, table_name, field, table_name, field)
    if option:
        sql += option

    sql += ')'
    safe_execute_commit_sql(conn, cur, sql, echo=echo)


def insert_many(conn, cur, table_name, action, list_values, commit=True,
                echo=False):
    if not list_values:
        return
    num_fields = len(list_values[0])
    sql = '%s %s values (%s)' % (action, table_name, 
                                 ','.join(['?'] * num_fields))

    if echo:
        print(sql)

    cur.executemany(sql, list_values)
    if commit:
        commit_or_rollback_raise(conn)


def _dict_insert_one(conn, cur, table_name, action, key_values, commit=True,
                echo=False):
    if not list_values:
        return

    keys = list(key_values.keys())
    values = list(key_values.values())

    key_string = ','.join(keys)
    value_string = ','.join([str(v) for v in values])
    num_fields = len(keys)

    sql = '%s %s (%s) values (%s)' % (action, table_name, 
                                      key_string, value_string)

    if echo:
        print(sql)

    cur.execute(sql)
    if commit:
        commit_or_rollback_raise(conn)
