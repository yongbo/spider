# -*- coding: utf-8 -*- 


import sqlite3
import logging

logger =  logging.getLogger()

class DataBase(object):

    def __init__(self,dbfile):
            
        #创建数据库链接
        self.conn = sqlite3.connect(dbfile)
        #设置支持中文存储
        self.conn.text_factory = str
        #自动提交
        self.conn.isolation_level = None
        self.cursor = self.conn.cursor()
        self.cursor.execute('create table if not exists spider_data( \
                            id INTEGER PRIMARY KEY AUTOINCREMENT,\
                            url text,\
                            key text,\
                            data text)') #数据使用二进制保存
        self.conn.commit()

	
    def save(self,data=[]):
		
        try:
            self.cursor.executemany("insert into spider_data (url,key,data) values (?,?,?)", data)
        except Exception,e:
            print str(e)
            logger.warn(str(e))

    def close(self):
        self.cursor.close()
        self.conn.close()
