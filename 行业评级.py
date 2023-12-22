import pandas as pd
from excess_predict import EXCESS_TEST
from max_count import MAX_COUNT
from tools.utils import get_idx_price
import datetime as dt
from dateutil.relativedelta import relativedelta
import os


class INDUSTRY_RANK:
    """
    行业排名
    """

    def __init__(self, max_indicator_li: list):
        # self.max_indicator_li = max_indicator_li
        exc_test = EXCESS_TEST()
        df_excess = exc_test.pre01_pct_all_delta
        df_excess.index = pd.to_datetime(df_excess.index)

        self.data_dict = {'公告预告超预期比例': df_excess}

        for ind in max_indicator_li:
            self.data_dict[ind] = MAX_COUNT(indicator=ind).pct_delta_df
            print(ind)

    def _industry_rank(self, est_quarter: str):
        """

        :param est_quarter: 指定季度yyyyQq
        """
        est_quarter = dt.datetime.date(pd.to_datetime(est_quarter) + relativedelta(months=3, days=-1))
        data_all = pd.DataFrame()
        for i in self.data_dict.keys():
            df = self.data_dict[i].loc[pd.to_datetime(est_quarter), :].to_frame()
            df.columns = [i]
            data_all = pd.concat([data_all, df], axis=1)
            print(i)

        data_all = data_all.drop(index=['综合', '综合金融', '商贸零售'])

        df_rank = data_all.rank(axis=0, ascending=False).astype(int)
        df_rank.loc[:, '总排名'] = df_rank.sum(axis=1).rank(axis=0, ascending=True)
        df_rank = df_rank.sort_values(by='总排名', ascending=True).astype(int)

        self.data_all = data_all.copy()
        self.df_rank = df_rank.copy()

    def calc_corr(self, est_quarter, start_date: str = str(dt.datetime.today()), ob_days: int = 365):
        """
        计算指定季度多个指标排名加总排名和未来指定区间行业受益排名的 spearman & pearson 相关系数
        :param est_quarter:指定季度
        :param start_date:区间收益起始日期
        :param ob_days: 收益区间长度
        :return:
        """
        self._industry_rank(est_quarter)
        end_date = pd.to_datetime(start_date).date() + dt.timedelta(days=ob_days)
        df_pct = get_idx_price(self.df_rank.index.to_list(), start_date=start_date, end_date=str(end_date),
                               method='int_pct') \
            .sort_values(by='区间涨跌幅', ascending=False)

        self.df_data = pd.merge(self.df_rank[['总排名']], df_pct, how='left', left_index=True, right_index=True)

        print(f'\n===== 回测区间{start_date}至{end_date} =====')
        print('\nspearman', self.df_data.corr(method='spearman') * (-1))
        print('\npearson', self.df_data.corr() * (-1))


if __name__ == '__main__':
    # eg:
    # S1 init 输入 财务指标list
    industry_rank_test = INDUSTRY_RANK(['单季_净利润同比', '单机_营业收入同比'])
    data = industry_rank_test.data_all
    # S2 输入指定季度yyyyQq 区间收益测算起始日 区间长度
    industry_rank_test.calc_corr(est_quarter='2020Q2', start_date='2020-09-01')
    industry_rank_test.calc_corr(est_quarter='2020Q3', start_date='2020-11-01', ob_days=120)
    df = industry_rank_test.df_data
