# -*- coding: UTF-8 -*-

from . import abs_db
#import pymysql
from sqlalchemy import create_engine

class mysql_db(abs_db.abs_db):
    def __init__(self, host, port, db, user, pwd):
        self.__host = host
        self.__port = port
        self.__db = db
        self.__user = user
        self.__pwd = pwd
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            #self.close()
            connstr = 'mysql+pymysql://{}:{}@{}:{}/{}?charset=utf8'.format(self.__user, self.__pwd, self.__host, self.__port, self.__db)
            print('db connect str:{}'.format(connstr))
            self.conn = create_engine(connstr)
            # set autocommit?
            # self.cursor = self.conn.cursor()
        except:
            print('db connect fail, {},{},{},{},{}'.format(self.__host, self.__port, self.__db, self.__user, self.__pwd))

    def close(self):
        pass
    def execute(self, sql):
        pass
'''
    def close(self):
        if self.conn != None:
            self.cursor.close()
            self.cursor = None
            self.conn.close()
            self.conn = None

    def execute(self, sql):
        return self.cursor.execute(sql)
'''
