# -*- coding: UTF-8 -*-

import importlib
from settings import settings

def get_db(dbsetting):
    db_setting = settings[dbsetting]
    module_name = '{}_db'.format(db_setting['DB_TYPE'])
    module = importlib.import_module('.', module_name)
    cls = getattr(module, module_name)
    db = cls(db_setting['HOST'],
            db_setting['PORT'],
            db_setting['DB_NAME'],
            db_setting['USER'],
            db_setting['PWD'])
    db.connect()
    
    return db

marketdb = get_db('MARKET_DB')
