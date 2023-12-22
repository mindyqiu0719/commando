#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 23 10:24:02 2021

@author: xingjiayuan
"""
import pandas as pd
import datetime as dt
import pymssql
from tools.utils_v2 import get_stk_indus, vtrade_date


class GET_WHITELIST:
    def __init__(self, stock_code=None, end_date=str(dt.date.today()), observed_days=60, density1=0):
        """

        :param end_date: 观测结束日期
        :param observed_days:前推日期
        :param density1:有效点百分比
        """
        self.stock_code=stock_code
        self.end_date = end_date
        # self.start_date = (pd.Timestamp(self.end_date) - pd.Timedelta(100, 'd')).strftime('%Y-%m-%d')
        self.observed_days = observed_days
        self.density1 = density1
        self.engine = pymssql.connect(server='192.168.1.35', user='wen', password='wen', charset='GBK')

    def Get_industry(self):
        print('S1 获取行业')
        indus1df = get_stk_indus(date=self.end_date, level=1)[
            ['stock_code', 'industry_code', 'industry_name']].set_index('stock_code')
        indus2df = get_stk_indus(date=self.end_date, level=2)[
            ['stock_code', 'industry_code', 'industry_name']].set_index('stock_code')
        indus2df.columns = ['sec_industry_code', 'sec_industry_name']
        indus3df = get_stk_indus(date=self.end_date, level=3)[
            ['stock_code', 'industry_code', 'industry_name']].set_index('stock_code')
        indus3df.columns = ['thd_industry_code', 'thd_industry_name']
        self.industry = pd.concat([indus1df, indus2df, indus3df], axis=1).reset_index()

    def Get_fcst(self):
        print('S2 获取一致预期')
        start_date = vtrade_date(self.end_date, -(self.observed_days + 10)).date()

        if self.stock_code is None:
            sql_con_fcst = f"SELECT stock_code,stock_name,con_date,con_year,con_or,con_or_type FROM dbo.con_forecast_stk " \
                       f"where con_date <= '{self.end_date}' and con_date >='{start_date}' " \
                       f"ORDER BY stock_code, con_date, con_year"
        else:
            sql_con_fcst = f"SELECT stock_code,stock_name,con_date,con_year,con_or,con_or_type FROM dbo.con_forecast_stk " \
                       f"where con_date <= '{self.end_date}' and con_date >='{start_date}' " \
                           f"and stock_code='{self.stock_code}'" \
                       f"ORDER BY stock_code, con_date, con_year"

        self.fcst_all = pd.read_sql_query(sql_con_fcst, con=self.engine)  # 获取期间全部一致预期日频数据
        self.fcst_all['con_or'] /= 10000
        self.fcst_all['con_date'] = self.fcst_all['con_date'].map(lambda x: pd.Timestamp(x))  # 预期日的格式为字符串，修改格式为日期格式
        real_data = self.fcst_all.loc[self.fcst_all['con_or_type'] == 0, :].groupby(['stock_code', 'con_date']).tail(
            1).reset_index(drop=True)  # con_or_type=0为年报真实数据，但一个预期日内可能有近两年真实数据，取后一个，即最近一年的年报数据
        self.fcst_all['real_data'] = self.fcst_all.merge(real_data, how='left', on=['stock_code', 'con_date'])[
            'con_or_y']  # 将真实数据的列添加到总表中
        # self.fcst_all.dropna(inplace = True)
        self.fcst_all_wo_0 = self.fcst_all.mask(self.fcst_all['con_or_type'] == 0).dropna()  # 去除真实数据行，保留预期数据
        self.fcst_all_wo_0 = self.fcst_all_wo_0.groupby(['stock_code', 'con_date']).head(2).reset_index(
            drop=True)  # 取最近两年的一致预期数据
        self.fcst_all = self.fcst_all_wo_0.groupby(['stock_code', 'con_date']).tail(1).reset_index(
            drop=True)  # 去除最近一年的一致预期数据，保留第二年的一致预期数据

        # 获取当年一致预期数据，并去除con_or_type不为1的数据
        self.con_or_this_year = self.fcst_all_wo_0.groupby(['stock_code', 'con_date']).head(1).reset_index(drop=True)
        # self.con_or_this_year.mask(self.con_or_this_year['con_or_type']!=1,inplace=True)
        self.fcst_all['con_or_this_year'] = \
            self.fcst_all.merge(self.con_or_this_year, how='left', on=['stock_code', 'con_date'])[
                'con_or_y']  # 将第一年的一致预期加入总表
        self.fcst_all = self.fcst_all.merge(self.industry, on='stock_code', how='left')

    def Get_market_value(self):
        sql_market_value = '''
        SELECT stock_code,tcap FROM dbo.qt_stk_daily where trade_date = '{0:s}';
        '''.format(self.fcst_all['con_date'].max().strftime('%Y-%m-%d'))
        self.market_value = pd.read_sql_query(sql_market_value, con=self.engine)
        self.market_value['tcap'] /= 10000
        self.fcst_all = self.fcst_all.merge(self.market_value, on='stock_code', how='left')
        self.df = self.fcst_all.copy()

    def Get_density(self):
        # 将con_or_type不为1的设置成空值
        self.fcst_all[['con_or_type']] = self.fcst_all.mask(self.fcst_all['con_or_type'] != 1)[['con_or_type']]
        self.fcst_all['valid_pct'] = (self.fcst_all.groupby(['stock_code'])
                                      .rolling(self.observed_days, min_periods=self.observed_days)
                                      .count()['con_or_type'].reset_index().set_index(['stock_code'])['con_or_type'] /
                                      self.fcst_all.groupby(['stock_code'])
                                      .rolling(self.observed_days, min_periods=self.observed_days)
                                      .count()[['con_date']].reset_index().set_index(['stock_code'])[
                                          'con_date']).reset_index()[0]
        # 计算有效数据比例
        # self.fcst_all.dropna(inplace=True)
        self.fcst_all = self.fcst_all.reset_index(drop=True)
        self.fcst_all = self.fcst_all.groupby('stock_code').tail(1).reset_index(drop=True)[
            ['stock_code', 'stock_name', 'con_date', 'industry_name', 'sec_industry_name', 'thd_industry_name',
             'tcap', 'con_year', 'real_data', 'con_or_this_year', 'con_or', 'valid_pct']]

    def Get_whitelist(self):
        self.whitelist = self.fcst_all.loc[self.fcst_all['valid_pct'] > self.density1, :].reset_index(drop=True)

    def Run(self):
        self.Get_industry()
        self.Get_fcst()
        self.Get_market_value()
        self.Get_density()
        self.Get_whitelist()

        if self.stock_code is None:
            return self.whitelist
        else:
            if len(self.whitelist):
                return True
            else:
                return False

