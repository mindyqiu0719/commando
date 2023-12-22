import datetime as dt
import pandas as pd
import pymssql
from tools.utils import get_idx_price


class INDUS_BACKTEST:
    def __init__(self, indus_df, ob_days: int = 365):
        """

        :param indus_df: industry_name,signal_date:datetime,(optional column: rank)
        :param ob_days: 测试区间
        """
        self.engine = pymssql.connect(server='192.168.1.35',
                                      user='wen', password='wen',
                                      database='KMD_HSGT_FUNDS', charset='GBK')
        self.ob_days = ob_days
        self.indus_df = indus_df.copy()

    def get_data(self):
        self.indus_df['signal_date']=pd.to_datetime(self.indus_df['signal_date'])
        tmp = pd.read_sql_query("select distinct industry_code,industry_name from citic_industry_index", self.engine)

        date_li = self.indus_df['signal_date'].drop_duplicates().to_list()
        data_all = pd.DataFrame()
        for d in date_li:
            data_df = self.indus_df[self.indus_df['signal_date'] == d]
            end_date = str((pd.to_datetime(d) + dt.timedelta(days=self.ob_days)).date())
            indus_price_df = \
                get_idx_price(idx_list=data_df['industry_name'].to_list(), start_date=str(d.date()), end_date=end_date,
                              method='int_pct').reset_index()
            all_indus = get_idx_price(start_date=str(d.date()), end_date=end_date, method='rank').reset_index()
            data_df = data_df.merge(indus_price_df, on='industry_name', how='left') \
                .merge(all_indus, on='industry_name', how='left').assign(end_date=end_date)
            data_all = pd.concat([data_all, data_df], axis=0)

        data_all = data_all.merge(tmp, how='left', on='industry_name')
        data_all = data_all[['industry_name', 'industry_code', 'signal_date', 'end_date', '区间涨跌幅', '区间涨跌幅排名']]
        return data_all


if __name__ == '__main__':
    print('')
    # df = pd.DataFrame(columns={'industry_name', 'signal_date'})

    #ib = INDUS_BACKTEST(indus_df=df)
    #res = ib.get_data()
