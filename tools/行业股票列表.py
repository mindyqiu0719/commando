# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 13:46:56 2020

@author: Wen
"""

import pymssql
import pandas as pd


# 提取板块内的股票
class STK_LIST:
    """
    输入日期提取行业板块内的全部股票
    """
    def __init__(self, date, indus_code):
        self.date = pd.to_datetime(date).strftime('%F')
        self.indus_code = pd.Series(indus_code)
        self.engine = pymssql.connect(server='192.168.1.35',
                                      user='wen', password='wen',
                                      database='ggbase', charset='GBK'
                                      )

    def Get_stk(self):
        sql_stk = "select stock_code, stock_name, standard_name,industry_name, into_date, out_date, industry_code, industry_level " \
                  "from qt_indus_constituents where 1=1 " \
                  "and industry_code in ({0:s}) " \
                  "and into_date <= '{1:s}' " \
                  "and (out_date >= '{1:s}' or out_date is null) " \
                  "order by stock_code" \
            .format(str(self.indus_code.to_list()).replace("[", "").replace("]", ""), self.date)
        # print(sql_stk)
        self.df = pd.read_sql_query(sql_stk, self.engine)

    def Run(self):
        self.Get_stk()
        return self.df
