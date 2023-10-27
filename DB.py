# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 16:13:14 2023

@author: ur08366
"""
import cx_Oracle, psycopg2, logging, re
from sqlalchemy import create_engine
import mysql.connector
import pandas as pd
import pyodbc 
import numpy as np
# 设置 Oracle 客户端路径
try:
    cx_Oracle.init_oracle_client(lib_dir=r'D:\UR08366\instantclient_11_2')
except:
    pass

class DbConnector:
    def __init__(self, db_info):
        self.db_type = db_info["db_type"]
        self.database_info = db_info["database_info"]
        self.user_info = db_info["user_name"]
        self.password_info = db_info["password"]
        self.host_info = db_info["host"]
        self.port_info = db_info["port"]
        if self.db_type == 'oracle':
            dsn_tns = cx_Oracle.makedsn(self.host_info, self.port_info, service_name=self.database_info)
            self.connection = cx_Oracle.connect(user=self.user_info, password=self.password_info, dsn=dsn_tns,
                                                encoding='UTF-8')
            self.cursor = self.connection.cursor()
            dsn = f"oracle://{self.user_info}:{self.password_info}@{self.host_info}:{self.port_info}/{self.database_info}"
            # 创建一个数据库引擎
            self.engine = create_engine(dsn)
        elif self.db_type == 'postgres':
            self.connection = psycopg2.connect(host=self.host_info, user=self.user_info, password=self.password_info, database=self.database_info)
            self.cursor = self.connection.cursor()
            self.engine = create_engine('postgresql://' + self.user_info + ':' + self.password_info + '@'
                                        + self.host_info + ':' + self.port_info + '/' + self.database_info, echo=False)
        elif self.db_type == 'mysql':
            self.connection = mysql.connector.connect(user=self.user_info, password=self.password_info,
                                          host=self.host_info,
                                          database=self.database_info)
            self.cursor = self.connection.cursor()
            self.engine = create_engine(
                f'mysql+pymysql://{self.user_info}:{self.password_info}@{self.host_info}/{self.database_info}',
                echo=False)
        elif self.db_type == 'sqlserver':
            self.connection = pyodbc.connect(f'DRIVER=SQL Server;SERVER={self.host_info};DATABASE={self.database_info};UID={self.user_info};PWD={self.password_info}')
            self.cursor = self.connection.cursor()
            conn_str = (f"Driver=SQL Server;Server={self.host_info};Database={self.database_info};UID={self.user_info};PWD={self.password_info};")
            self.engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}", echo=False)

    def close_cursor(self):
        try:
            self.cursor.close()
        except Exception as err:
            logging.info(f'{err}')

    def close_connection(self):
        try:
            self.connection.close()
        except Exception as err:
            logging.info(f'{err}')

    def read_from_db(self, query_list):
        if self.db_type == 'oracle':
            try:
                query_result_list = []
                for query in query_list:
                    self.cursor.execute(query)
                    data = self.cursor.fetchall()
                    cols = list(map(lambda x: x[0], self.cursor.description))
                    dataframe = pd.DataFrame(data, columns=cols)
                    query_result_list.append(dataframe)
                    self.connection.commit()
            except (Exception, cx_Oracle.Error) as error:
                logging.info("Error while connecting to Oracle, The error is{}".format(error))
                self.cursor.execute("ROLLBACK")
            finally:
                if len(query_result_list) == 0:
                    return [pd.DataFrame([])]
                else:
                    return query_result_list
        elif self.db_type == 'postgres':
            try:
                query_result_list = []
                for query in query_list:
                    self.cursor.execute(query)
                    data = self.cursor.fetchall()
                    cols = list(map(lambda x: x[0], self.cursor.description))
                    dataframe = pd.DataFrame(data, columns=cols)
                    query_result_list.append(dataframe)
                    self.connection.commit()
            except (Exception, cx_Oracle.Error) as error:
                self.cursor.execute("ROLLBACK")
                logging.info("Error while connecting to postgres, The error is\n{}".format(error))
            finally:
                if len(query_result_list) == 0:
                    return [pd.DataFrame([])]
                else:
                    return query_result_list
        else:
            try:
                query_result_list = []
                for query in query_list:
                    self.cursor.execute(query)
                    data = self.cursor.fetchall()
                    cols = list(map(lambda x: x[0], self.cursor.description))
                    try:
                        dataframe = pd.DataFrame(data, columns=cols)
                    except:
                        data = [list(e) for e in data]
                        dataframe = pd.DataFrame(data, columns=cols)
                    query_result_list.append(dataframe)
                    self.connection.commit()
            except Exception as error:
                self.cursor.execute("ROLLBACK")
                logging.info("Error while connecting to else db, The error is \n{}".format(error))
            finally:
                if len(query_result_list) == 0:
                    return [pd.DataFrame([])]
                else:
                    return query_result_list

    def upload_to_db(self, dataframe, schema, table):
        if self.db_type == 'postgres':
            # logging.info("# Upload to：{}".format(schema))
            # create query
            col_list = list(dataframe.columns)
            col_str = ','.join(col_list)
            insert_query = """ INSERT INTO {}.{} ({}) VALUES""".format(schema, table, col_str)
            dataframe = dataframe.astype(str)
            dataframe = str(tuple(dataframe.itertuples(index=False, name=None)))
            if len(col_list) == 1:
                raise AssertionError("這裡有問題")
                # dataframe = dataframe[1:-1].replace(',', "")
            else:
                dataframe = dataframe[1:-1]  # 去掉前後括號
            insert_query = insert_query + dataframe
            try:
                self.cursor.execute(insert_query)
                self.connection.commit()
            except (Exception, psycopg2.Error) as error:
                raise AssertionError("Error while connecting to PostgreSQL", error)
        else:
            raise ValueError("No support")

    def upload_to_db_not_exist(self, dataframe, table, schema, primary_key):
        if isinstance(primary_key, list):
            primary_key_copy = primary_key
            primary_key = ""
            for key in primary_key_copy:
                primary_key += "\"" + key + "\"" + ','
            primary_key = primary_key.rstrip(',')
        if self.db_type == 'postgres':
            col_list = list(dataframe.columns)
            col_str = "("
            for s in col_list:
                col_str += "\"" + s + "\"" + ","  #
            col_str = col_str.rstrip(',')
            col_str += ")"
            df = dataframe.copy()
            df.reset_index(drop=True, inplace=True)
            dataframe = dataframe.astype(str)
            if dataframe.shape[0] == 1:
                one_tag = True
            else:
                one_tag = False
            if one_tag:
                dataframe = str(tuple(dataframe.itertuples(index=False, name=None)))[1:-1].rstrip(',')
            else:
                dataframe = str(tuple(dataframe.itertuples(index=False, name=None)))[1:-1]
            insert_query = """INSERT into {}.{} {} values {} ON CONFLICT ({}) DO NOTHING;""".format(
                schema, table, col_str, dataframe, primary_key)
            try:
                self.cursor.execute(insert_query)
                self.connection.commit()
            except Exception as err:
                logging.info(f"{insert_query}")
                logging.info(f"{err}")
        else:
            raise ValueError("No support")

    def upinset_to_db(self, dataframe, table, schema, primary_key):
        df = dataframe.copy()
        df.reset_index(drop=True, inplace=True)
        if isinstance(primary_key, list):
            primary_key_copy = primary_key
            primary_key = ""
            for key in primary_key_copy:
                primary_key += "\"" + key + "\"" + ','
            primary_key = primary_key.rstrip(',')
        else:
            raise AssertionError("必須輸入list")
        if self.db_type == 'postgres':
            col_list = list(dataframe.columns)
            col_str = "("
            for s in col_list:
                col_str += "\"" + s + "\"" + ","  #
            col_str = col_str.rstrip(',')
            col_str += ")"
            # m = (df['訂單號碼'] == '0Z227004-01')
            # df.loc[m, 'UNI_KEY'] = "測試"
            for idx, row in df.iterrows():
                dataframe = df.iloc[idx:idx + 1, :].astype(str)
                if dataframe.shape[0] == 1:
                    dataframe = str(tuple(dataframe.itertuples(index=False, name=None)))[1:-1].rstrip(',')
                else:
                    dataframe = str(tuple(dataframe.itertuples(index=False, name=None)))[1:-1]
                # 製作更新條件
                update_condition = ""
                for k in primary_key_copy:
                    update_condition += f""" AND {schema}.{table}."{k}" = '{row[k]}'"""
                update_condition = update_condition.lstrip(' AND')
                insert_query = f"""INSERT into {schema}.{table} {col_str} values {dataframe} ON CONFLICT ({primary_key}) DO UPDATE SET {col_str} = {dataframe}
                                                WHERE {update_condition}"""
                self.cursor.execute(insert_query)
                self.connection.commit()
                try:
                    pass
                except Exception as error:
                    logging.info(error)
        else:
            col_list = list(dataframe.columns)
            col_str = "("
            for s in col_list:
                col_str += "\"" + s + "\"" + ","  #
            col_str = col_str.rstrip(',')
            col_str += ")"
            for idx, row in df.iterrows():
                dataframe = df.iloc[idx:idx + 1, :].astype(str)
                if dataframe.shape[0] == 1:
                    dataframe = str(tuple(dataframe.itertuples(index=False, name=None)))[1:-1].rstrip(',')
                else:
                    dataframe = str(tuple(dataframe.itertuples(index=False, name=None)))[1:-1]
                # 製作更新條件
                update_condition = ""
                for k in primary_key_copy:
                    update_condition += f""" AND {schema}.{table}."{k}" = '{row[k]}'"""
                update_condition = update_condition.lstrip(' AND')
                # insert_query = f"""INSERT into {schema}.{table} {col_str} values {dataframe} ON CONFLICT ({primary_key}) DO UPDATE SET {col_str} = {dataframe}
                #                                             WHERE {update_condition}"""
                # {schema}.
                insert_query = f"""INSERT INTO {table} {col_str} VALUES {dataframe} ON DUPLICATE KEY UPDATE {col_str} = {dataframe};"""
                self.cursor.execute(insert_query)
                self.connection.commit()
                try:
                    pass
                except Exception as error:
                    logging.info(error)

    def exe_sql(self, query_list):
        if self.db_type == 'oracle':
            query_result_list = []
            for query in query_list:
                query = query.upper()
                select_sql_str = re.findall(string=query, pattern=r"SELECT")
                try:
                    self.cursor.execute(query)
                except Exception as err:
                    self.cursor.execute("ROLLBACK")
                    return [err]
                if len(select_sql_str) != 0:
                    data = self.cursor.fetchall()
                    cols = list(map(lambda x: x[0], self.cursor.description))
                    dataframe = pd.DataFrame(data, columns=cols)
                    query_result_list.append(dataframe)
                else:
                    query_result_list.append(True)
                self.connection.commit()
            return query_result_list
        elif self.db_type == 'postgres':
            try:
                query_result_list = []
                for query in query_list:
                    query = query.upper()
                    select_sql_str = re.findall(string=query, pattern=r"SELECT")
                    self.cursor.execute(query)
                    if len(select_sql_str) != 0:
                        data = self.cursor.fetchall()
                        cols = list(map(lambda x: x[0], self.cursor.description))
                        dataframe = pd.DataFrame(data, columns=cols)
                        query_result_list.append(dataframe)
                    else:
                        query_result_list.append(True)
                    self.connection.commit()
            except (Exception, cx_Oracle.Error) as error:
                self.cursor.execute("ROLLBACK")
                logging.info("Error while connecting to postgres, The error is\n{}".format(error))
            finally:
                if len(query_result_list) == 0:
                    return [pd.DataFrame([])]
                else:
                    return query_result_list
        elif self.db_type == 'sqlserver':
            query_result_list = []
            for query in query_list:
                select_sql_str = re.findall(string=query, pattern=r"SELECT")
                select_sql_str2 = re.findall(string=query, pattern=r"select")
                try:
                    self.cursor.execute(query)
                except Exception as err:
                    self.cursor.execute("ROLLBACK")
                    logging.info("Error while connecting to sqlserver, The error is\n{}".format(err))
                    return [False]
                if len(select_sql_str) != 0 | len(select_sql_str2) != 0:
                    data = self.cursor.fetchall()
                    cols = list(map(lambda x: x[0], self.cursor.description))
                    dataframe = pd.DataFrame(data, columns=cols)
                    query_result_list.append(dataframe)
                else:
                    query_result_list.append(True)
                self.connection.commit()
            return query_result_list