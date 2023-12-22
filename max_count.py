import pandas as pd
from tools.财务信息 import SuntimeData as sd
from tools.utils import get_stk_indus
import datetime as dt
from dateutil.relativedelta import relativedelta


class MAX_COUNT:
    """
    计算财务指标连续n个季度/年份最大值公司个数占其所在行业的比例
    """

    def __init__(self, indicator: str, rolling_year=2):
        """

        :param indicator: 财务指标
        :param est_quarter: 衡量季度 yyyyQq-2021Q3
        :param rolling_year: 滚动计算年份
        """
        self.indicator = indicator
        self.rolling_quarter = rolling_year * 4
        self.raw_raw_data, self.raw_data = self._get_data()

    def _get_data(self):
        """
        获取全部原始数据
        :return:
        """
        stk_all = get_stk_indus()
        indus_li = stk_all['中信证券一级行业'].drop_duplicates().to_list()
        stk_code_li = stk_all['stock_code'].to_list()

        data_all = pd.DataFrame()
        n = [i for i in range(0, len(stk_all), 500)] + [len(stk_all)]
        for i in range(len(n) - 1):
            # print(i, n[i], n[i + 1])
            data_df = sd(stk_code_li[n[i]:n[i + 1]], [self.indicator], start_year=2012,
                         report_quarter=[1, 2, 3, 4]).Run()
            data_all = pd.concat([data_all, data_df])

        date_li = data_all['日期'].drop_duplicates().to_list()
        # date_li = [x.date() for x in date_li]
        date_li.sort()

        data_all1 = pd.DataFrame()
        for d in date_li:
            # print(d)
            df = data_all[data_all['日期'] == d]
            indus_df = get_stk_indus(date=str(d.date())).rename(columns={'stock_code': '股票代码'})
            dff = df.merge(indus_df[['股票代码', '中信证券一级行业']], how='left', on='股票代码')
            data_all1 = pd.concat([data_all1, dff])

        self.indus_pivot = data_all1.pivot_table(columns='日期', index='中信证券一级行业', values='股票代码', aggfunc='count') \
            .sort_index(axis=0, ascending=True).sort_index(axis=1).T

        res_all = pd.DataFrame()
        dff_all = pd.DataFrame()
        for indus in indus_li:
            data = data_all1[data_all1['中信证券一级行业'] == indus].sort_values(by=['股票代码', '日期'])
            data1 = data.pivot(columns='股票代码', index='日期', values=self.indicator)
            data1_max = data1.rolling(self.rolling_quarter).max()
            df_mask = data1_max == data1
            df = df_mask.applymap(lambda x: 1 if x == True else 0)
            res = df.sum(axis=1).to_frame().rename(columns={0: indus})
            df = df.sort_index(ascending=False).T.reset_index()
            dff = data.drop_duplicates('股票代码')[['股票名称', '股票代码']].merge(df, on='股票代码', how='left') \
                .assign(中信证券一级行业=indus)
            dff_all = pd.concat([dff_all, dff], axis=0)
            res_all = pd.concat([res_all, res], axis=1)

        self.res_copy = res_all.copy()
        self.res_copy = self.res_copy.sort_index(axis=1, ascending=True)
        self.pct_df = self.res_copy / self.indus_pivot
        self.pct_yoy_df = self.pct_df.pct_change(freq='4Q')
        self.pct_delta_df = self.pct_df - self.pct_df.shift(4)

        dff_all = dff_all.set_index(['股票代码', '股票名称', '中信证券一级行业']).T.sort_index()
        return data_all, dff_all

    def res_to_excel(self):
        """
        存储数据到Excel path为 \\yyyy-mm-dd 指标名称 滚动季度数 Q max_count.xlsx
        :return:
        """
        wr = pd.ExcelWriter(f'{dt.date.today()} {self.indicator} {self.rolling_quarter}Q max_count.xlsx')

        self.pct_delta_df.to_excel(wr, sheet_name='分行业占比同比差')
        self.pct_yoy_df.to_excel(wr, sheet_name='分行业占比同比')
        self.pct_df.to_excel(wr, sheet_name='分行业占比')
        # res_all.to_excel(wr, sheet_name='count_sumup')
        # indus_pivot.to_excel(wr, sheet_name='indus_sumup')
        wr.save()

    def get_head_indus(self, est_quarter=None, head_n=5):
        """
        获取 指定季度日期的 行业前N名
        :param est_quarter: 指定季度日期yyyyQq，默认最近一个季度
        :param head_n: 前N名行业
        :return:
        """
        if est_quarter:
            est_quarter = dt.datetime.date(pd.to_datetime(est_quarter) + relativedelta(months=3, days=-1))
        else:
            date_li = self.pct_delta_df.index.to_list()
            date_li.sort()
            est_quarter = date_li[-1]

        print(f"===== {self.indicator}{self.rolling_quarter}Q同比差{est_quarter}前{head_n}行业 =====")
        print(self.pct_delta_df.loc[pd.to_datetime(est_quarter)].nlargest(head_n))

    def get_indus_raw(self, indus_name_li=None, est_quarter=None):
        """
        获取 指定季度日期 指定行业 的原始数据
        :param indus_name_li: 中信证券一级行业名称列表，默认全部
        :param est_quarter: 指定季度日期yyyyQq，默认最近一个季度
        :return:
        """
        self.raw_data1 = self.raw_data.T.reset_index()
        if est_quarter:
            est_quarter = pd.to_datetime(est_quarter) + relativedelta(months=3, days=-1)
        else:
            date_li = self.pct_delta_df.index.to_list()
            date_li.sort()
            est_quarter = date_li[-1]

        if indus_name_li is None:
            indus_name_li = self.raw_data1['中信证券一级行业'].drop_duplicates().to_list()

        indus_raw_data = self.raw_data1[
                             (self.raw_data1['中信证券一级行业'].isin(indus_name_li))].loc[:,
                         ['股票代码', '股票名称', '中信证券一级行业'] + [est_quarter]]

        indus_df = get_stk_indus(date=est_quarter, level=[2, 3]).rename(columns={'stock_code': '股票代码'})
        indus_df.pop('stock_name')
        indus_raw_data = indus_raw_data \
            .merge(self.raw_raw_data[self.raw_raw_data['日期'] == est_quarter][['股票代码', self.indicator]], on='股票代码') \
            .merge(indus_df, on='股票代码', how='left')
        indus_raw_data.sort_values(by=['中信证券一级行业', '中信证券二级行业', '中信证券三级行业', '股票代码'], inplace=True)

        indus_raw_data = indus_raw_data.reset_index(drop=True)

        indus_raw_data = indus_raw_data[['股票代码', '股票名称', est_quarter, self.indicator,
                                         '中信证券一级行业', '中信证券二级行业', '中信证券三级行业']]
        return indus_raw_data


if __name__ == '__main__':
    # eg:
    # S1: 输入 财务指标(可参照朝阳永续指标.xlsx)
    max_count = MAX_COUNT(indicator='单季_毛利率')
    # S2 输入 评价的季度yyyyQq 和 前N名 可重复修改参数直接获取数据 data=max_count.pct_delta_df
    max_count.get_head_indus(est_quarter='2021Q3', head_n=5)
    # S3 想获取的行业原始数据
    df = max_count.get_indus_raw(indus_name_li=['食品饮料', '家电'], est_quarter='2021Q3')
    # 存储数据到excel
    max_count.res_to_excel()

    from tools.标准化行业回测程序 import INDUS_BACKTEST

    data_rank = max_count.pct_delta_df.rank(axis=1, ascending=False).stack().reset_index()
    data_rank.columns = ['signal_date', 'industry_name', 'rank']
    ib = INDUS_BACKTEST(indus_df=data_rank)
    res = ib.get_data()
