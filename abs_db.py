# -*- coding: UTF-8 -*-

import abc

class abs_db(metaclass = abc.ABCMeta):
    '''
    抽象的db接口:
    1、connect
    2、close
    4、excute
    '''
    @abc.abstractmethod
    def connect(self):
        pass

    @abc.abstractmethod
    def close(self):
        pass

    @abc.abstractmethod
    def execute(self, sql):
        pass

    @classmethod
    def __subclasshook__(cls, C):
        if cls is abs_db:
            attrs = set(dir(C))
            if set(cls.__abstractmethods__) <= attrs:
                return True

        return NotImplemented
