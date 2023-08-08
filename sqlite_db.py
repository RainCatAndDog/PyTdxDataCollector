# -*- coding: UTF-8 -*-

import abs_db
import sqlite3
from settings import settings
import os

class sqlite_db(abs_db.abs_db):
    def __init__(self, host, port, db, user, pwd):
        self.__host = host
        self.__port = port
        self.__db = os.path.join(settings['WORK_SPACE'], db)
        self.__user = user
        self.__pwd = pwd
        self.conn = None
        self.cursor = None

    def connect(self):
        try:
            self.close()
            #print('closed')
            self.conn = sqlite3.connect(self.__db) 
            #self.conn.isolation_level = None # set autocommit
            #print('connected')
            self.cursor = self.conn.cursor()
            #print('cursor gotted')
        except:
            print('db connect fail, {},{},{},{},{}'.format(self.__host, self.__port, self.__db, self.__user, self.__pwd))

    def close(self):
        if self.conn != None:
            self.cursor.close()
            self.cursor = None
            self.conn.close()
            self.conn = None

    def execute(self, sql):
        return self.cursor.execute(sql)

