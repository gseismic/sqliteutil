import config
from sqliteutil import Database, Table


def test_table_1():
    db_file = 'db_test.db'
    db = Database(db_file)
    table_dict = [{'name': 'id', 'dtype': 'text', 'primary': True},
                  {'name': 'name', 'dtype': 'text', 'primary': True},
                  {'name': 'gender', 'dtype': 'text', 'index': True},
                  {'name': 'age', 'dtype': 'integer', 'nullable': False}]

    table_name = 'table1'
    table = Table(db, table_name, table_dict, 
                  auto_commit=True, echo=True)
    print('all index', table.get_all_index())
    table.drop_table()
    print('drop all index ..')
    table.drop_all_index()
    table.create_table() # not create index
    table.create_index(ignore_if_exists=True)
    table.dict_insert({'id': 1, 'name': 'lsl', 'age': '18'})
    table.dict_insert({'id': 2, 'name': 'lzl', 'age': 0})
    table.commit()


def test_table_2():
    db_file = 'db_test.db'
    db = Database(db_file)
    table_dict = [{'name': 'id', 'dtype': 'text', 'primary': True},
                  {'name': 'name', 'dtype': 'text', 'primary': True},
                  {'name': 'gender', 'dtype': 'text', 'index': True},
                  {'name': 'age', 'dtype': 'integer', 'nullable': False}]

    table_name = 'table2'
    table = Table(db, table_name, table_dict, 
                  auto_commit=True, echo=True)
    table.create_table()
    table.dict_insert({'id': 1, 'name': 'lsl1', 'age': '18'})
    table.dict_insert({'id': 2, 'name': 'lzl1', 'age': 0})
    table.commit()


def test_table_update1():
    db_file = 'db_test.db'
    db = Database(db_file)
    table_dict = [{'name': 'id', 'dtype': 'text', 'primary': True},
                  {'name': 'name', 'dtype': 'text', 'primary': True},
                  {'name': 'gender', 'dtype': 'text', 'index': True},
                  {'name': 'age', 'dtype': 'integer', 'nullable': False}]

    table_name = 'table2'
    table = Table(db, table_name, table_dict, 
                  auto_commit=True, echo=True)
    table.create_table(ignore_if_exists=True)
    table.create_index(ignore_if_exists=True)
    table.dict_update({'id': 1, 'name': 'lsl1111', 'age': '18'}, where='name="lsl1"')
    table.dict_update({'id': 1, 'name': 'lzl1111', 'age': '18'}, where='name="lzl1"')
    table.commit()
    data = table.dict_select(fields=None, where='name="lsl1111"')
    print(data)
    list_kv = []
    list_kv.append({'id': 10, 'name': 'lsl1111', 'age': '18'})
    list_kv.append({'id': 11, 'name': 'lsl1111', 'age': '18'})
    list_kv.append({'id': 12, 'name': 'lsl1111', 'age': '18'})
    table.dict_insertmany(list_kv, action='insert or ignore into')
    table.commit()


if __name__ == "__main__":
    if 0:
        test_table_1()
    if 0:
        test_table_2()
    if 1:
        test_table_update1()
