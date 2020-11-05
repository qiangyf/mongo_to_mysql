"""
mongo数据自动建表嵌入mysql数据库
统一使用固定的mysql数据类型
"""


import requests
import json
import pymongo
import pymysql
import random



def get_table_fields(collection_name,mongo_db):
    client = pymongo.MongoClient('localhost', 27017)
    collection = client[mongo_db][collection_name]
    a = collection.find_one()
    table_fields = [i[0] for i in a.items()]
    table_fields = table_fields[1:]
    return table_fields

def get_mongo_data(collection_name,mongo_db):
    client = pymongo.MongoClient('localhost',27017)
    collection = client[mongo_db][collection_name]
    datas = []
    for x in collection.find({},{'_id':0}):
        data = [str(i[1]) for i in x.items()]
        data = tuple(data)
        datas.append(data)
    return datas

def connect_mysql(mysql_db):
    db = pymysql.connect('localhost', 'root', '', mysql_db)
    cur = db.cursor()
    return db, cur


def make_sql_create_str(table_name,table_fields,data_type):
    
    
    sql_create_1 = 'create table if not exists {}(id int(11) auto_increment,'.format(table_name)
    sql_create_3 = 'primary key(id));'
    table_fields_list = []
    for t in table_fields:
        table_fields_list.append(t + ' ' +data_type)
    sql_create_2 = ','.join(table_fields_list) + ','
    sql_create = sql_create_1 + sql_create_2 + sql_create_3
    print(sql_create)
    return sql_create

def creat_mysql(table_name,table_fields,data_type,mysql_db):
    conn, cur = connect_mysql(mysql_db)
    sql_create = make_sql_create_str(table_name,table_fields,data_type)
    # print(sql_create)
    cur.execute(sql_create)
    # conn.commit()
    cur.close()
    conn.close()


def make_sql_str(table_name,table_fields):
    sql = 'insert into tb_domain_infos ({}) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'.format(table_fields)
    sql = 'insert into tb_serve_platform_domain_info (domainName,serviceContent,ircsId,setMode,regId,userName,userId,regType,ircsName,isLocalProvince) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    sql_1 = 'insert into {} '.format(table_name)
    
    sql_2 = "({})".format(','.join(table_fields))
    
    ss = ''
    for i in range(len(table_fields)):
        ss = ss + '%s,'
    sql_3 = ' value({})'.format(ss)[:-2] + ')'
    sql = sql_1 + sql_2 + sql_3
    print('sql:',sql)
    return sql

def save_mysql(datas,table_name,table_fields,mysql_db):
    conn, cur = connect_mysql(mysql_db)
    sql = make_sql_str(table_name,table_fields)
    cur.executemany(sql, datas)
    conn.commit()
    cur.close()
    conn.close()

#mongo 库名和表名
mongo_db = 'test'
collection_name = 'test_4'
#mysql 库名和表名   必须现创建好库，表是自动创建
mysql_db = 'test'
table_name = 'test_4'
#mysql统一字段类型
data_type = 'mediumtext'
table_fields = get_table_fields(collection_name,mongo_db)
print(table_fields)
creat_mysql(table_name,table_fields,data_type,mysql_db)
datas = get_mongo_data(collection_name,mongo_db)
save_mysql(datas,table_name,table_fields,mysql_db)
