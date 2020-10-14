from sqliteutil import Database, Table


def test_local_db():
    db_file = 'db_test.db'
    db = Database(db_file)


def test_local_dir_db():
    db_file = 'not_exist/db_test.db'
    db = Database(db_file)


def test_rootpath_db():
    db_file = '/home/lsl/not_exist/db_test.db'
    db = Database(db_file)


if __name__ == "__main__":
    if 1:
        test_local_db()
    if 1:
        test_local_dir_db()
    if 1:
        test_rootpath_db()
    # pass
