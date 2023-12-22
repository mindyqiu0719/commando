# -*- coding: utf-8 -*-
"""
Created on Tue Jul 20 11:20:05 2021

modified by myqiu on Wed Oct 13 2021

@author: gongyong
"""
import datetime as dt
import pandas as pd
import pymssql
from tools.utils import vtrade_date, get_stk_indus


class SIGNAL_BACKTEST:
    """
    回测计算器

    """

    def __init__(self, stk_code_list: list = None,
                 start_date: str = str((dt.datetime.today() - dt.timedelta(days=365)).date()),
                 ob_days: int = 365):
        """

        :param stk_code_list: 股票代码列表,默认None为起始日期去除新股和ST全部A股
        :param start_date: 起始日期
        :param ob_days: 观测区间
        """

        self.engine = pymssql.connect(server='192.168.1.35',
                                      user='wen', password='wen',
                                      database='ggbase', charset='GBK')
        self.start_date = vtrade_date(start_date)  # 获取起始日期最近交易日

        if stk_code_list:
            self.stk_code_list = stk_code_list
        else:
            self.stk_code_list = get_stk_indus(date=start_date)['stock_code'].to_list()

        self.end_date = self.start_date + dt.timedelta(days=ob_days)
        if self.end_date >= dt.date.today():
            self.end_date = vtrade_date(date=str(dt.date.today()), offset_n=-1)

        self.signal = pd.DataFrame(data={'stock_code': self.stk_code_list}) \
            .assign(signal_date=self.start_date,
                    end_date=self.end_date)

        self.stk_code_str = "'" + "','".join(self.stk_code_list) + "'"

    def Get_stk(self):
        # 获取股票数据========================================================
        sql_stk = f'''select trade_date,stock_name,stock_code,tclose,lclose,matiply_ratio,volume,tcap  \
                  from qt_stk_daily  \
                  where stock_code in ({self.stk_code_str})  \
                  and trade_date>='{self.signal['signal_date'].min().strftime('%Y-%m-%d')}'  \
                  and trade_date<='{self.end_date}'  \
                  order by stock_code,trade_date
                  '''

        self.stk = pd.read_sql_query(sql_stk, con=self.engine)
        self.stk['tclose'] = self.stk['tclose'].mask(self.stk['volume'] == 0)  # 将停盘日的数据设为空值
        self.stk['close'] = self.stk.groupby(['stock_code']).apply(
            lambda df_: df_['tclose'] * df_['matiply_ratio'] / df_['matiply_ratio'].iat[-1]).reset_index(
            drop=True)  # 除权
        self.stk['tot_cap'] = self.stk['tcap'] / self.stk['close']  # 计算总股本
        self.stk['close'] = self.stk['close'].fillna(method='ffill')
        # self.stk = self.stk.loc[self.stk['stock_code'].isin(self.signal['stock_code']) == True, :].reset_index(
        # drop=True)  # 获取信号池中股票的日频数据

    def Get_max_return(self):
        # 获取从买入时点的最大收益和到达该收益花费的天数和日均收益率============
        self.stk['max_return'] = self.stk.groupby('stock_code').apply(
            lambda df_: df_['close'].expanding().max().max() / df_['close'] - 1).reset_index(drop=True)  # 获取从买入到最高点的收益
        self.stk['days_to_max'] = self.stk.groupby('stock_code').apply(
            lambda df_: df_.loc[df_['close'] == df_['close'].max(), 'trade_date'].max() - df_[
                'trade_date']).reset_index(drop=True).map(lambda x: x.days)
        self.stk['daily_return'] = self.stk['max_return'] / self.stk['days_to_max']  # 计算日均涨幅（按自然日）

    def Get_max_drawdown(self):
        # 获取最大回撤===========================================
        self.stk['max_to_here'] = self.stk.groupby('stock_code').apply(
            lambda df_: df_['close'].expanding().max()).reset_index(drop=True)  # 计算股价“迄今为止”的最大值
        self.stk['drawdown'] = self.stk.groupby('stock_code').apply(
            lambda df_: df_['close'] / df_['max_to_here'] - 1).reset_index(drop=True)  # 计算回撤
        self.max_drawdown = self.stk.groupby('stock_code').min().reset_index()
        self.stk['max_drawdown'] = self.stk.merge(self.max_drawdown, on='stock_code', how='left')[
            'drawdown_y']  # 将最大回撤并入表格

    def Get_sharp_ratio(self):
        # 获取sharp ratio========================================
        self.stk['monthly_return'] = self.stk.groupby('stock_code').apply(
            lambda df_: df_['close'].pct_change(20)).reset_index(drop=True)  # 滚动计算20个交易日的收益率
        self.monthly_sharp_ratio = self.stk.groupby('stock_code').apply(
            lambda df_: df_.loc[df_.index % 20 == 0, 'monthly_return'].mean()) \
                                   / self.stk.groupby('stock_code').apply(lambda df_: df_.loc[
            df_.index % 20 == 0, 'monthly_return'].std())  # 计算20个交易日收益率的均值和标准差的比值，即sharp ratio，假设无风险利率为零
        self.monthly_sharp_ratio = self.monthly_sharp_ratio.to_frame('sharp_ratio').reset_index()
        self.stk['sharp_ratio'] = self.stk.merge(self.monthly_sharp_ratio, how='left', on='stock_code')['sharp_ratio']

    def Get_1year_100(self):
        # 提取结果所需要的列=====================================
        self.result = \
            self.signal.merge(self.stk, left_on=['stock_code', 'signal_date'], right_on=['stock_code', 'trade_date'],
                              how='left')[
                ['stock_code', 'stock_name', 'max_return', 'days_to_max', 'daily_return',
                 'max_drawdown', 'sharp_ratio', 'signal_date', 'end_date']]
        self.result['one_year_100pct'] = (self.result['max_return'].map(lambda x: x > 1)) & (
            self.result['daily_return'].map(lambda x: x > (1 / 365)))
        self.result = self.result.sort_values(by=['one_year_100pct', 'max_return'], ascending=False).reset_index(
            drop=True)
        self.result = self.result[
            ['stock_code', 'stock_name', 'one_year_100pct', 'max_return', 'days_to_max', 'daily_return',
             'max_drawdown', 'sharp_ratio', 'signal_date', 'end_date']]

    def Run(self):
        print(f"回测区间:{self.start_date.date()}至{self.end_date.date()}\n"
              f"股票数量:{len(self.signal)}\n")
        self.Get_stk()
        self.Get_max_return()
        self.Get_max_drawdown()
        self.Get_sharp_ratio()
        self.Get_1year_100()

        print(f"True:{self.result['one_year_100pct'].value_counts().loc[True]} "
              f"False:{self.result['one_year_100pct'].value_counts().loc[False]}")

        df = get_stk_indus(date=self.start_date, level=[1, 2, 3])
        df.pop('stock_name')
        self.result = self.result.merge(df, how='left', on='stock_code')
        return self.result


if __name__ == '__main__':
    test1 = SIGNAL_BACKTEST().Run()
