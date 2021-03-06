import config
import pytest
from sqliteutil import Database, Table


def test_local_db():
    db_file = 'db_test.db'
    db = Database(db_file)
    print('db_file', db_file)
    db.close()


def test_local_dir_db():
    db_file = 'not_exist/db_test.db'
    db = Database(db_file)
    db.close()


def test_rootpath_db():
    with pytest.raises(PermissionError):
        db_file = '/not_exist/db_test.db'
        db = Database(db_file)


if __name__ == "__main__":
    if 1:
        test_local_db()
    if 1:
        test_local_dir_db()
    if 1:
        test_rootpath_db()
