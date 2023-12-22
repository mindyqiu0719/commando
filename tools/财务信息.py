import pandas as pd
import os
import pymssql
from dateutil.relativedelta import relativedelta
import numpy as np
from tools.utils import vtrade_date


class SuntimeData:
    """
    从朝阳永续数据库提取数据，需要读取朝阳永续指标.xlsx
    """

    def __init__(self, stk_code_list, fin_indicator_list, start_year=2000, report_quarter=[1, 2, 3, 4]):
        self.conn = pymssql.connect(server='192.168.1.35',
                                    user='wen', password='wen',
                                    database='ggbase', charset='GBK')
        self.stk_code_list = stk_code_list
        self.fin_indicator_list = fin_indicator_list
        self.report_quarter = report_quarter
        self.start_year = start_year

        self.report_quarter_str = ",".join([str(1000 + x) for x in self.report_quarter])
        self.stk_code_str = "'" + "','".join(self.stk_code_list) + "'"

    def stk_tmp(self):

        sql_cmd = f"select stock_code,stock_name,report_year,report_quarter " \
                  f"from fin_income_gen " \
                  f"where 1=1 and report_type=1001 " \
                  f"and stock_code in ({self.stk_code_str}) " \
                  f"and report_year >={self.start_year}" \
                  f"and report_quarter in ({self.report_quarter_str})"

        data = pd.read_sql_query(sql_cmd, self.conn)
        data = data[data['report_year'] >= self.start_year].reset_index(drop=True)
        data.columns = ['股票代码', '股票名称', '年份', '季度']
        data['日期'] = None
        for i, r in data.iterrows():
            data.loc[i, '日期'] = pd.to_datetime(pd.to_datetime(str(r['年份']) + 'Q' + str(r['季度'])[-1])
                                               + relativedelta(months=3, days=-1))
        data['日期'] = data['日期'].astype('datetime64[ns]')
        self.stk_tmp_df = data.copy()

        return self.stk_tmp_df

    def fin_main_ratio(self, indicator_list, indicator_dict):
        """
        indicator_dict_calc = {'营业周期': ['inv_turnover', 'ar_turnover']}
        indicator_dict.update(indicator_dict_calc)
        """
        col_list, fin_indicator = self._indicator_list_convert(indicator_list, indicator_dict)

        tmp = self.stk_tmp()
        sql_cmd = f"select stock_code,report_year, report_quarter, {fin_indicator} " \
                  f"from fin_main_ratio where 1=1 and report_type=1001 " \
                  f"and stock_code in ({self.stk_code_str}) " \
                  f"and report_quarter in ({self.report_quarter_str})"

        df_data = pd.read_sql_query(sql_cmd, self.conn)
        df_data.columns = ['股票代码', '年份', '季度'] + col_list

        if '营业周期' in indicator_list:
            df_data['营业周期'] = None

            for i, r in df_data.iterrows():
                if all([r['存货周转率'] > 0, r['应收帐款周转率'] > 0]):
                    df_data.loc[i, '营业周期'] = 36500 / r['存货周转率'] + 36500 / r['应收帐款周转率']
                elif any([r['存货周转率'] > 0, r['应收帐款周转率'] > 0]):
                    if r['应收帐款周转率'] > 0:
                        df_data.loc[i, '营业周期'] = 36500 / r['应收帐款周转率']
                    else:
                        df_data.loc[i, '营业周期'] = 36500 / r['存货周转率']
                else:
                    df_data.loc[i, '营业周期'] = np.nan

        if '营收五年复合增长' in indicator_list:
            df = df_data.pivot(columns='股票代码', index='年份', values='营收五年复合增长') + 100
            df1 = df.rolling(window=5).agg(lambda x: np.prod(x)).pow(1 / 5) - 100
            df2 = df1.stack().reset_index().rename(columns={0: '营收五年复合增长'})
            df_data.pop('营收五年复合增长')
            df_data = df_data.merge(df2, how='left', on=['股票代码', '年份'])

        res = tmp.merge(df_data, how='left', on=['股票代码', '年份', '季度']) \
            .sort_values(by=['股票代码', '年份', '季度'])
        res = res[['股票代码', '年份', '季度'] + indicator_list]
        return res

    def fin_income_gen(self, indicator_list, indicator_dict):
        """
        indicator_dict_calc = {
            '销售毛利率': ['tor1', 'toc1'],
            '研发费用率': ['tor1', 'toc14'],
            '毛利减三费': ['tor1', 'toc1', 'toc10', 'toc11', 'toc12']}

        indicator_dict.update(indicator_dict_calc)
        """
        col_list, fin_indicator = self._indicator_list_convert(indicator_list, indicator_dict)
        tmp = self.stk_tmp()
        sql_cmd = f"select stock_code,report_year, report_quarter, {fin_indicator} " \
                  f"from fin_income_gen where 1=1 and report_type=1001 " \
                  f"and stock_code in ({self.stk_code_str}) " \
                  f"and report_quarter in ({self.report_quarter_str})"

        df_data = pd.read_sql_query(sql_cmd, self.conn)
        df_data.columns = ['股票代码', '年份', '季度'] + col_list

        if '销售毛利率' in indicator_list:
            df_data = df_data.assign(销售毛利率=lambda x: round((x['营业收入'] - x['营业成本']) / x['tor1'], 4) * 100)

        if '研发费用率' in indicator_list:
            df_data = df_data.assign(研发费用率=lambda x: round(x['研发费用'] / x['tor1'], 4) * 100)

        if '毛利减三费' in indicator_list:
            df_data = df_data.assign(
                毛利减三费=lambda x: round((x['tor1'] - x['toc1'] - x['toc10'] - x['toc11'] - x['toc12']) / x['tor1'],
                                      4) * 100)

        res = tmp.merge(df_data, how='left', on=['股票代码', '年份', '季度']).sort_values(by=['股票代码', '年份', '季度'])
        res = res[['股票代码', '年份', '季度'] + indicator_list]

        return res

    def fin_income_single(self, indicator_list, indicator_dict):
        col_list, fin_indicator = self._indicator_list_convert(indicator_list, indicator_dict)
        tmp = self.stk_tmp()
        sql_cmd = f"select stock_code,report_year, report_quarter, {fin_indicator} " \
                  f"from fin_income_single where 1=1 and report_type=1001 " \
                  f"and stock_code in ({self.stk_code_str}) " \
                  f"and report_quarter in ({self.report_quarter_str})"

        df_data = pd.read_sql_query(sql_cmd, self.conn)
        df_data.columns = ['股票代码', '年份', '季度'] + col_list
        df_data['季度'] = df_data['季度'].map(lambda x: int(x))
        df_data['日期'] = None
        for i, r in df_data.iterrows():
            df_data.loc[i, '日期'] = pd.to_datetime(pd.to_datetime(str(r['年份']) + 'Q' + str(r['季度'])[-1])
                                                  + relativedelta(months=3, days=-1))

        if '单季_净利润同比' in indicator_list:
            df = df_data.pivot(columns='股票代码', index='日期', values='np')
            df.index = pd.to_datetime(df.index)
            df1 = df.pct_change(freq='4Q')
            df2 = df1.stack().reset_index().rename(columns={0: '单季_净利润同比'})
            df_data['日期'] = pd.to_datetime(df_data['日期'])
            df_data = df_data.merge(df2, how='left', on=['股票代码', '日期'])
            # df_data.pop('日期')

        if '单季_营业收入同比' in indicator_list:
            df = df_data.pivot(columns='股票代码', index='日期', values='tor1')
            df.index = pd.to_datetime(df.index)
            df1 = df.pct_change(freq='4Q')
            df2 = df1.stack().reset_index().rename(columns={0: '单季_营业收入同比'})
            df_data['日期'] = pd.to_datetime(df_data['日期'])
            df_data = df_data.merge(df2, how='left', on=['股票代码', '日期'])
            # df_data.pop('日期')

        if '单季_毛利率' in indicator_list:
            df_data = df_data.assign(单季_毛利率=lambda x: round((x['tor1'] - x['toc1']) / x['tor1'], 4) * 100)

        if '单季_销售费用率' in indicator_list:
            df_data = df_data.assign(单季_销售费用率=lambda x: round(x['toc10'] / x['tor1'], 4) * 100)

        if '单季_管理费用率' in indicator_list:
            df_data = df_data.assign(单季_管理费用率=lambda x: round(x['toc11'] / x['tor1'], 4) * 100)

        if '单季_财务费用率' in indicator_list:
            df_data = df_data.assign(单季_财务费用率=lambda x: round(x['toc12'] / x['tor1'], 4) * 100)

        if '单季_毛利减三费' in indicator_list:
            df_data = df_data.assign(单季_毛利减三费=lambda x:
            round((x['tor1'] - x['toc1'] - x['toc10'] - x['toc11'] - x['toc12']) / x['tor1'], 4) * 100)

        for col in col_list:
            df_data = df_data.rename(columns={col: indicator_dict['中文名'][col]})

        df_data['日期'] = df_data['日期'].astype('datetime64[ns]')
        res = tmp.merge(df_data, how='left', on=['股票代码', '年份', '季度', '日期']).sort_values(by=['股票代码', '年份', '季度'])
        res = res[['股票代码', '年份', '季度'] + indicator_list]

        return res

    def fin_balance_sheet_single(self, indicator_list, indicator_dict):
        col_list, fin_indicator = self._indicator_list_convert(indicator_list, indicator_dict)
        tmp = self.stk_tmp()
        sql_cmd = f"select stock_code,report_year, report_quarter, {fin_indicator} " \
                  f"from fin_balance_sheet_single where 1=1 and report_type=1001 and single_type=1001 " \
                  f"and stock_code in ({self.stk_code_str}) " \
                  f"and report_quarter in ({self.report_quarter_str})"

        df_data = pd.read_sql_query(sql_cmd, self.conn)
        df_data.columns = ['股票代码', '年份', '季度'] + col_list
        df_data['季度'] = df_data['季度'].map(lambda x: int(x))
        df_data['日期'] = None
        for i, r in df_data.iterrows():
            df_data.loc[i, '日期'] = pd.to_datetime(pd.to_datetime(str(r['年份']) + 'Q' + str(r['季度'])[-1])
                                                  + relativedelta(months=3, days=-1))

        res = tmp.merge(df_data, how='left', on=['股票代码', '年份', '季度']).sort_values(by=['股票代码', '年份', '季度'])
        res = res[['股票代码', '年份', '季度'] + indicator_list]

        return res

    def fin_balance_sheet_gen(self, indicator_list, indicator_dict):
        """
        indicator_dict_calc = {'在建工程占比': ['ta', 'ta_nca7'],
                               '预收及合同负债': ['tl_cl6', 'tl_cl18']}
        indicator_dict.update(indicator_dict_calc)
        """
        col_list, fin_indicator = self._indicator_list_convert(indicator_list, indicator_dict)
        tmp = self.stk_tmp()

        sql_cmd = f"select stock_code,report_year, report_quarter, {fin_indicator} " \
                  f"from fin_balance_sheet_gen where 1=1 and report_type=1001 " \
                  f"and stock_code in ({self.stk_code_str}) " \
                  f"and report_quarter in ({self.report_quarter_str})"

        df_data = pd.read_sql_query(sql_cmd, self.conn)
        df_data.columns = ['股票代码', '年份', '季度'] + col_list

        if '在建工程占比' in indicator_list:
            df_data = df_data.assign(在建工程占比=lambda x: round((x['在建工程']) / x['总资产'], 4) * 100)

        if '预收及合同负债' in indicator_list:
            df_data = df_data.assign(预收及合同负债=lambda x: x['预收款项'].fillna(0) + x['合同负债'].fillna(0))

        res = tmp.merge(df_data, how='left', on=['股票代码', '年份', '季度']).sort_values(by=['股票代码', '年份', '季度'])
        res = res[['股票代码', '年份', '季度'] + indicator_list]

        return res

    def qt_stk_daily(self, indicator_list, indicator_dict):
        """
        indicator_dict_calc = {}
        indicator_dict.update(indicator_dict_calc)
        """
        col_list, fin_indicator = self._indicator_list_convert(indicator_list, indicator_dict)

        tmp = self.stk_tmp()
        tmp = tmp.assign(交易日=lambda x: x['日期'].map(lambda y: vtrade_date(y))).sort_values(by='交易日')

        trade_date_list = tmp['交易日'].drop_duplicates().to_list()
        start_date = trade_date_list[0].date()
        end_date = trade_date_list[-1].date()

        sql_cmd = f"select stock_code,trade_date,{fin_indicator} " \
                  f"from qt_stk_daily where 1=1 " \
                  f"and stock_code in ({self.stk_code_str}) " \
                  f"and trade_date >= '{start_date}' " \
                  f"and trade_date <= '{end_date}'"

        df_data = pd.read_sql_query(sql_cmd, self.conn)
        df_data.columns = ['股票代码', '交易日'] + col_list
        df_data = df_data.assign(流通市值=lambda x: x['流通市值'] / 10000)

        res = tmp.merge(df_data, how='left', on=['股票代码', '交易日']).sort_values(by=['股票代码', '年份'])
        res = res[['股票代码', '年份', '季度'] + indicator_list]
        return res

    def fin_rele_date(self, indicator_list, indicator_dict):
        col_list, fin_indicator = self._indicator_list_convert(indicator_list, indicator_dict)
        tmp = self.stk_tmp()

        sql_cmd = f"select stock_code,report_year, report_quarter, {fin_indicator} " \
                  f"from fin_rele_date where 1=1 and report_type=1001 " \
                  f"and stock_code in ({self.stk_code_str}) " \
                  f"and report_quarter in ({self.report_quarter_str})"

        df_data = pd.read_sql_query(sql_cmd, self.conn)
        df_data.columns = ['股票代码', '年份', '季度'] + col_list

        res = tmp.merge(df_data, how='left', on=['股票代码', '年份', '季度']).sort_values(by=['股票代码', '年份', '季度'])
        res = res[['股票代码', '年份', '季度'] + indicator_list]

        return res

    @staticmethod
    def _indicator_list_convert(indicator_list, indicator_dict):
        """

        :param indicator_list: 中文指标名称列表
        :param indicator_dict: 中文指标名称所对应的数据库字段
        :return: df列名称，数据库查询字段
        """
        indicator_list_copy = []
        for ind_name in indicator_list:
            for ind_name_value in indicator_dict['中文名'].keys():
                if ind_name == indicator_dict['中文名'][ind_name_value]:
                    indicator_list_copy.append(ind_name_value)
                    print(ind_name_value)

        select_ind = []
        for ind in indicator_list_copy:
            if pd.notna(indicator_dict['计算字段'][ind]):
                calc_ind = indicator_dict['计算字段'][ind].split(',')
                select_ind = select_ind + calc_ind
            else:
                select_ind.append(ind)
        fin_ind_list = list(set(select_ind))
        # 合并指标
        if len(fin_ind_list) > 1:
            fin_indicator_str = ','.join(fin_ind_list)
        else:
            fin_indicator_str = fin_ind_list[0]

        return fin_ind_list, fin_indicator_str

    @staticmethod
    def _indicator_workbook2dict(indicator_file):
        # indicator_file = 'tools/朝阳永续指标.xlsx'
        sheets_name_list = pd.ExcelFile(indicator_file).sheet_names
        dict_all = {}
        for sn in sheets_name_list:
            indicator_df = pd.read_excel(indicator_file, sheet_name=sn)
            # indicator_df['字段名'] = indicator_df['字段名'].map(lambda x: x.split(',')).map(lambda y: y[0] if len(y) == 1 else y)
            dict_all[sn] = indicator_df[['字段名', '中文名', '计算字段']].set_index('字段名').to_dict()
        return dict_all

    def Run(self):

        indicator_file_path = os.getcwd() + '/tools/朝阳永续指标.xlsx'

        all_indicators = self._indicator_workbook2dict(indicator_file_path)

        df_all = self.stk_tmp()

        # fin_income_gen
        ind_dict = all_indicators['fin_income_gen']
        ind_list = [x for x in self.fin_indicator_list if x in [d for d in ind_dict['中文名'].values()]]
        if ind_list:
            df = self.fin_income_gen(ind_list, ind_dict)
            df_all = df_all.merge(df, how='left', on=['股票代码', '年份', '季度'])

        # fin_income_single
        ind_dict = all_indicators['fin_income_single']
        ind_list = [x for x in self.fin_indicator_list if x in [d for d in ind_dict['中文名'].values()]]
        if ind_list:
            df = self.fin_income_single(ind_list, ind_dict)
            df_all = df_all.merge(df, how='left', on=['股票代码', '年份', '季度'])

        # fin_main_ratio
        ind_dict = all_indicators['fin_main_ratio']
        ind_list = [x for x in self.fin_indicator_list if x in [d for d in ind_dict['中文名'].values()]]
        if ind_list:
            df = self.fin_main_ratio(ind_list, ind_dict)
            df_all = df_all.merge(df, how='left', on=['股票代码', '年份', '季度'])

        # fin_balance_sheet_gen
        ind_dict = all_indicators['fin_balance_sheet_gen']
        ind_list = [x for x in self.fin_indicator_list if x in [d for d in ind_dict['中文名'].values()]]
        if ind_list:
            df = self.fin_balance_sheet_gen(ind_list, ind_dict)
            df_all = df_all.merge(df, how='left', on=['股票代码', '年份', '季度'])

        # fin_balance_sheet_single
        ind_dict = all_indicators['fin_balance_sheet_single']
        ind_list = [x for x in self.fin_indicator_list if x in [d for d in ind_dict['中文名'].values()]]
        if ind_list:
            df = self.fin_balance_sheet_single(ind_list, ind_dict)
            df_all = df_all.merge(df, how='left', on=['股票代码', '年份', '季度'])

        # qt_stk_daily
        ind_dict = all_indicators['qt_stk_daily']
        ind_list = [x for x in self.fin_indicator_list if x in [d for d in ind_dict['中文名'].values()]]
        if ind_list:
            df = self.qt_stk_daily(ind_list, ind_dict)
            df_all = df_all.merge(df, how='left', on=['股票代码', '年份', '季度'])

        # fin_rele_date
        ind_dict = all_indicators['fin_rele_date']
        ind_list = [x for x in self.fin_indicator_list if x in [d for d in ind_dict['中文名'].values()]]
        if ind_list:
            df = self.fin_rele_date(ind_list, ind_dict)
            df_all = df_all.merge(df, how='left', on=['股票代码', '年份', '季度'])

        # =========================
        df_all = df_all[['股票代码', '股票名称', '年份', '季度', '日期'] + self.fin_indicator_list]
        df_all = df_all.sort_values(by=['股票代码', '年份', '季度']).reset_index(drop=True)
        return df_all
