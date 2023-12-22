import pandas as pd
import pymssql
import datetime as dt
from tools.utils import vtrade_date, is_listed, get_stk_indus
from dateutil.relativedelta import relativedelta


class EXCESS_TEST:
    """
    超预期行业占比同比差
    """

    def __init__(self):
        self.conn = pymssql.connect(server='192.168.1.35',
                                    user='wen', password='wen',
                                    database='ggmore', charset='GBK')
        self.raw_data = self._get_data()

    def _get_data(self):
        sql_cmd = "select stock_code,stock_name,report_year,report_quarter,declare_date,appraisal_date,is_prediction," \
                  "excess_type,act_np,con_np,excess_rate,organ_num,excess_on " \
                  "from der_excess_stock " \
                  "where report_year>=2012 and excess_standard=0"

        data = pd.read_sql_query(sql_cmd, self.conn)
        data = data.sort_values(by=['stock_code', 'report_year', 'report_quarter']).reset_index(drop=True)
        for i, r in data.iterrows():
            data.loc[i, '日期'] = pd.to_datetime(pd.to_datetime(str(r['report_year']) + 'Q' + str(r['report_quarter']))
                                               + relativedelta(months=3, days=-1))

        date_li = [d.date() for d in data['日期'].drop_duplicates().to_list()]
        date_li.sort()
        data['日期'] = [d.date() for d in data['日期']]
        indus_all = pd.DataFrame()
        for d in date_li:
            stk_indus = get_stk_indus(date=str(d), level=[1, 2, 3]).assign(日期=d)
            indus_all = pd.concat([indus_all, stk_indus], axis=0)
        indus_all.pop('stock_name')

        '''
        if self.est_quarter:
            self.est_quarter = dt.datetime.date(pd.to_datetime(self.est_quarter) + relativedelta(months=3, days=-1))
        else:
            self.est_quarter = date_li[-1]
        '''

        # 合并行业
        data1 = data.merge(indus_all, how='left', on=['stock_code', '日期'])
        # self.raw_data = data1.copy()

        # 公告超预期公司数量
        data_pre0 = data1[data1['is_prediction'] == 0] \
            .drop_duplicates(['stock_code', 'report_year', 'report_quarter'], keep='last') \
            .pivot_table(index='日期', columns='中信证券一级行业', values='excess_type', aggfunc='sum').fillna(0)
        # 预告超预期公司数量
        data_pre1 = data1[data1['is_prediction'] == 1] \
            .drop_duplicates(['stock_code', 'report_year', 'report_quarter'], keep='last') \
            .pivot_table(index='日期', columns='中信证券一级行业', values='excess_type', aggfunc='sum').fillna(0)
        # 有公告公司数量
        indus_count_pre0 = data1[data1['is_prediction'] == 0] \
            .drop_duplicates(['stock_code', 'report_year', 'report_quarter'], keep='last') \
            .pivot_table(index='日期', columns='中信证券一级行业', values='stock_name', aggfunc='count')
        # 有预告公司数量
        indus_count_pre1 = data1[data1['is_prediction'] == 1] \
            .drop_duplicates(['stock_code', 'report_year', 'report_quarter'], keep='last') \
            .pivot_table(index='日期', columns='中信证券一级行业', values='stock_name', aggfunc='count')
        # 全行业公司数量
        indus_all_count = indus_all.pivot_table(index='日期', columns='中信证券一级行业', values='stock_code',
                                                aggfunc='count')

        pre0_pct = data_pre0 / indus_count_pre0  # 公告超预期占有公告公司的比例
        pre1_pct = data_pre1 / indus_count_pre1  # 预告超预期占有预告公司的比例
        self.pre0_pct_all = data_pre0 / indus_all_count  # 公告超预期占全行业公司的比例
        self.pre1_pct_all = data_pre1 / indus_all_count  # 预告超预期占全行业公司的比例
        self.pre01_pct_all = (self.pre0_pct_all + self.pre1_pct_all) / 2
        self.pre0_pct_all_delta = self.pre0_pct_all - self.pre0_pct_all.shift(4)  # 公告超预期比例同比变动
        self.pre1_pct_all_delta = self.pre1_pct_all - self.pre1_pct_all.shift(4)  # 预告超预期比例同比变动
        self.pre01_pct_all_delta = self.pre01_pct_all - self.pre01_pct_all.shift(4)

        return data1
        '''
        # 百分位排名
        pre01_pct = (pre0_pct + pre1_pct) / 2
        pre01_pct0 = pre01_pct.rank(pct=True, axis=0)
        pre01_pct1 = pre01_pct.rank(pct=True, axis=1)
        pre0_pct_pct0 = pre0_pct.rank(pct=True, axis=0)
        pre0_pct_pct1 = pre0_pct.rank(pct=True, axis=1)
        pre1_pct_pct0 = pre1_pct.rank(pct=True, axis=0)
        pre1_pct_pct1 = pre1_pct.rank(pct=True, axis=1)
        '''

    def res_to_excel(self):
        wr = pd.ExcelWriter(f'{dt.date.today()} excess_test.xlsx')

        self.pre01_pct_all.to_excel(wr, sheet_name='公告预告超预期行业占比')
        self.pre01_pct_all_delta.to_excel(wr, sheet_name='公告预告超预期同比差')
        self.pre0_pct_all.to_excel(wr, sheet_name='公告超预期行业占比')
        self.pre0_pct_all_delta.to_excel(wr, sheet_name='公告超预期同比差')
        self.pre1_pct_all.to_excel(wr, sheet_name='预告超预期行业占比')
        self.pre1_pct_all_delta.to_excel(wr, sheet_name='预告超预期同比差')

        wr.save()

    def get_head_indus(self, est_quarter=None, head_n=5):
        """

        :param est_quarter: 评估季度 yyyyQq-2021Q3 默认最新一个季度
        :param head_n: 选取前n个行业 默认5
        :return:
        """
        if est_quarter:
            est_quarter = dt.datetime.date(pd.to_datetime(est_quarter) + relativedelta(months=3, days=-1))
        else:
            date_li = self.pre01_pct_all_delta.index.to_list()
            date_li.sort()
            est_quarter = date_li[-1]

        print(f"===== 预告超预期同比差前{head_n}行业 =====")
        print(self.pre1_pct_all_delta.loc[est_quarter].nlargest(head_n))
        print(f"===== 公告超预期同比差前{head_n}行业 =====")
        print(self.pre0_pct_all_delta.loc[est_quarter].nlargest(head_n))
        print(f"===== 预告公告/2超预期同比差前{head_n}行业 =====")
        print(self.pre01_pct_all_delta.loc[est_quarter].nlargest(head_n))

    def get_indus_raw(self, is_pre: int = None, is_exc: int = None, indus_name_li: list = None,
                      est_quarter: str = None):
        """
        :param is_pre: 0公告 1预告
        :param is_exc: 0未超预期 1超预期
        :param indus_name_li: 筛选行业 默认全部
        :param est_quarter: 评估季度 默认最新一个季度
        :return:
        """

        if est_quarter:
            est_quarter = dt.datetime.date(pd.to_datetime(est_quarter) + relativedelta(months=3, days=-1))
        else:
            date_li = self.pre01_pct_all_delta.index.to_list()
            date_li.sort()
            est_quarter = date_li[-1]

        if indus_name_li:
            indus_li = []
            for indus in indus_name_li:
                indus_li.append(indus)
        else:
            indus_li = self.raw_data['中信证券一级行业'].drop_duplicates().to_list()

        indus_raw_data = self.raw_data[
            (self.raw_data['中信证券一级行业'].isin(indus_li)) & (self.raw_data['日期'] == est_quarter)]
        # & (self.raw_data['is_prediction'] == is_pre) & (self.raw_data['excess_type'] == is_exc)]

        if is_pre is not None:
            indus_raw_data = indus_raw_data[indus_raw_data['is_prediction'] == is_pre]
        if is_exc is not None:
            indus_raw_data = indus_raw_data[indus_raw_data['excess_type'] == is_exc]

        indus_raw_data = indus_raw_data.sort_values(by=['中信证券一级行业', 'stock_code'])
        indus_raw_data = indus_raw_data[['stock_code', 'stock_name', '日期', 'is_prediction',
                                         'excess_type', 'act_np', 'con_np', 'excess_rate', 'organ_num',
                                         'excess_on', '中信证券一级行业', '中信证券二级行业', '中信证券三级行业']]

        return indus_raw_data


if __name__ == '__main__':
    # eg:
    # S1 init 获取数据
    exc_test = EXCESS_TEST()
    # S2 输入指定季度 yyyyQq 和 行业前N名
    exc_test.get_head_indus(head_n=5, est_quarter='2021Q2')
    # 输入 行业名称列表 和 指定季度 获取原始数据
    df = exc_test.get_indus_raw(indus_name_li=['电子', '食品饮料'], est_quarter='2021Q3')
    # 存储数据到Excel
    #exc_test.res_to_excel()

    from tools.标准化行业回测程序 import INDUS_BACKTEST

    data_rank = exc_test.pre01_pct_all_delta.rank(axis=1, ascending=False).stack().reset_index()
    data_rank.columns = ['signal_date', 'industry_name', 'rank']
    ib = INDUS_BACKTEST(indus_df=data_rank)
    res = ib.get_data()

# is_prediction 是否是业绩预告
## 0 年报或季报 1 业绩预告或快报

# con_np 一致预期净利润
## excess_standard=0时，为财报或者预告、快报发布日前三个交易日的一致预期净利润；
## excess_standard=1时，为对应财报期最后一个交易日的一致预期净利润。

# act_np 实际净利润或调整后一致预期净利润
## is_prediction=0时，对应年报真实数据或者季报发布日之后第3个交易日的一致预期；
## is_prediction=1时，对应预告的年报数据或者季报发布日之后第3个交易日的一致预期；

# excess_standard 判断标准
## 0 报表发布日超预期鉴定 1 报表发布周期超预期鉴定
