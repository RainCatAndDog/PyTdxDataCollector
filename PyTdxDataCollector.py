# -*- coding: UTF-8 -*-
import os
from os import path
from datetime import datetime
from pytdx.reader import TdxDailyBarReader
from pytdx.reader import GbbqReader
import db
import pandas as pd
from settings import settings

class TdxDataCollector():
    def __init__(self):
        '''
        获取数据目录下所有的文件名，生成股票清单
        沪市：vipdoc/sh/lday
        深市：vipdoc/sz/lday
        '''
        self.__data_path = path.join(settings['DATA_PATH'], 'vipdoc')
        self.__sh_stock_files = os.listdir(path.join(self.__data_path, 'sh/lday'))
        self.__sh_stock_files = [path.join(self.__data_path, 'sh/lday', stock) for stock in self.__sh_stock_files]
        # print(self.__sh_stock_files)
        self.__sz_stock_files = os.listdir(path.join(self.__data_path, 'sz/lday'))
        self.__sz_stock_files = [path.join(self.__data_path, 'sz/lday',stock) for stock in self.__sz_stock_files]
        self.__temp_file = os.path.join(settings['WORK_SPACE'], 'temp/runlog')
        self.__prepare()

    def __prepare(self):
        reader = GbbqReader()
        self.__gbbq = reader.get_df(path.join(settings['DATA_PATH'], 'T0002/hq_cache/gbbq'))

    def update_last_bars(self):
        '''
        根据当前日期和上次更新的日期，更新最近的数据
        '''
        
    def update_history(self, begin, end=None):
        '''
        读取日期在begin，end范围内的日K数据
        begin, 开始日期
        end，结束日期，如果为空则表示到当前为止
        '''
        # 这里要判断begin和end是否跨年，如果跨年了，要按年份分段处理，保证每张表只存储年内的数据
        if end == None:
            end = datetime.now().strftime('%Y-%m-%d')
        if begin > end:
            begin, end = end, begin
        #print('update range from {} to {}'.format(begin,end))
        date_sections = []
        dt_begin, dt_end = datetime.strptime(begin,'%Y-%m-%d'), datetime.strptime(end, '%Y-%m-%d')
        if dt_begin.year != dt_end.year:
            years = [y for y in range(dt_begin.year, dt_end.year+1)]
            begins = ['{}-{}'.format(years[i],'01-01') for i in range(1, len(years))]
            ends = ['{}-{}'.format(years[i],'12-30') for i in range(0, len(years)-1)]
            begins.insert(0, begin)
            ends.append(end)
            date_sections = [(begins[i],ends[i]) for i in range(0, len(begins))]
        else:
            date_sections = [(begin,end)]
        
        self.__update_exchange_history(self.__sh_stock_files, date_sections)
        self.__update_exchange_history(self.__sz_stock_files, date_sections)

    
    def __update_exchange_history(self, stock_files, date_sections):
        try:
            reader = TdxDailyBarReader(self.__data_path)
            for stock_file in stock_files:
                # 准备数据
                stock_type = reader.get_security_type(stock_file)
                if stock_type not in ["SH_A_STOCK","SZ_A_STOCK"]:
                    continue
                df = None    
                try:
                    df = reader.get_df(stock_file)
                except Exception as e:
                    print('读取{}时失败: {}'.format(stock_file, e.__class__.__name__))
                    continue
                    
                code = stock_file[-12:-4] 
                df['code'] = code
                df['prev_close'] = df.groupby('code')['close'].shift(1)
                df.iloc[0, -1] = df.iloc[0]['close']
                #df.to_csv('sz000651.csv')
                # 处理复权信息
                # 除权价=（除权前一日收盘价 + 配股价*配股比率 － 每股派息）/（1 + 配股比率 + 送股比率）
                try:
                    gbbq = self.__gbbq.groupby('code').get_group(code[2:])
                    for index, row in gbbq.iterrows():
                        date_time = datetime.strptime(str(row['datetime']), '%Y%m%d')
                        try:
                            if row['category'] == 1:
                                df.loc[str(date_time.date())]['prev_close'] = ( (df.loc[str(date_time.date())]['prev_close']
                                        - row['hongli_panqianliutong']/10 + row['peigujia_qianzongguben']*row['peigu_houzongguben']/10) 
                                        / (1 + (row['songgu_qianzongguben'] + row['peigu_houzongguben'])/10)
                                        )
                        except KeyError:
                            # print('{}没有{}的数据'.format(code, date_time))
                            continue
                except KeyError:
                    print('{}没有股本变迁信息'.format(code))
                '''
                前收盘价->涨跌幅->复权因子
                注意: 因为后复权价格不受后面除权除息的影响, 可以直接保存, 前复权价则要动态计算
                这里我用adjr(adjust_right)表示后复权(调整右边的), adjl(adjust_left)表示前复权(调整左边的)
                '''
                df['change'] = df['close'] / df['prev_close'] - 1
                df['adj_factor'] = (1 + df['change']).cumprod()
                df['close_adjL'] = df['adj_factor'] * (df.iloc[0]['close'] / df.iloc[0]['adj_factor'])
                df['close_adjR'] = df['adj_factor'] * (df.iloc[-1]['close'] / df.iloc[-1]['adj_factor']) 
                # 数据表按年组织
                for begin, end in date_sections:
                    real_begin = begin
                    year = begin[0:4]
                    last_update_date = self.__get_last_update_date(code, year)
                    #print(last_date)
                    if last_update_date != '':
                        real_begin = last_update_date
                        
                    df_target = df[real_begin:end]
                    
                    if len(df_target) > 1:
                        df_target.to_sql('daily_bar_{}'.format(year), db.marketdb.conn, if_exists='append')
                    else:
                        print('{}, {}~{}数据为空'.format(stock_file, begin, end))

        except NotImplementedError:
            print('pytdx无法识别{}的类型(沪市？深市？)'.format(stock_file))
        '''
        except Exception as e:
            print('处理{}时异常: {}'.format(stock_file, e.__class__.__name__))
        '''
            
    def __get_last_update_date(self, stock_code, year):
        '''
        Parameters
        ----------
        stock_code : string
            股票代码 
        year : string
            年份，用于确定访问哪一年的数据表

        Returns
        -------
        string
            日期字符串或者返回None

        '''
        try:
            sql = 'SELECT * FROM daily_bar_{} WHERE code="{}" order by date DESC limit 1'.format(year, stock_code)
            data = pd.read_sql_query(sql, db.marketdb.conn)
            if not data.empty:
                #print('获取到非空数据{}'.format(data.loc[0,'date']))
                return data.loc[0,'date'][0:10]
            else:
                return ''
        except Exception as e:
            #print('出现异常{}'.format(e.__class__.__name__))
            return ''
    '''
    def get_last_update_date(self, stock_code, year):
        return self.__get_last_update_date(stock_code, year)
    '''        
