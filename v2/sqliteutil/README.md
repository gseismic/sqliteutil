

# ChangeLog
    2020-04-20 14:47:28 v0.0.2

# ChangeLog
    close_db -> close()
    init_db -> init()
    table_dict -> table_fields
    2020-04-20 15:21:52 
        xxx_many: -> commit=True
        dict_insert_many -> dict_insertmany
        list_insertmany
        Database.commit
        Database.execute(self, sql) commit line removed
        table.drop -> database.drop_table
    exclude_autoindex -> default True

# BugFix
    2020-06-07 19:16:44 != -1 -> ==1 new_indices = [index for index in indices if index.find('autoindex') ==-1]
