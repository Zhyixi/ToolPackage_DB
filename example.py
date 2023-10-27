from DB import DbConnector

if __name__ == '__main__':

    # 以字典傳入連線資訊
    # 其中db_type目前有三種可用，
    db_info = {
        "host": "",
        "port": "1433",
        "database_info": "",
        "user_name": "",
        "password": "",
        "db_type": 'sqlserver',
    }
    db_connector = DbConnector(db_info)

    # 讀取資料表並且組成 pandas dataframe物件
    [df] = db_connector.read_from_db(query_list=["""Select * from schema.table"""])
    print(df)
    # 執行insert、create、alter等操作...供使用者彈性編寫sql語法，回傳值為成功與否的boolean
    # [success_flag] = db_connector.exe_sql(query_list=["""insert ...."""])
