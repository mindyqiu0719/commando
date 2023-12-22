import datetime as dt
import pandas as pd
import pymssql

conn1 = pymssql.connect(server='192.168.1.35',
                        user='wen', password='wen',
                        database='ggbase', charset='GBK')

conn2 = pymssql.connect(server='192.168.1.35',
                        user='wen', password='wen',
                        database='KMD_HSGT_FUNDS', charset='GBK')


def vtrade_date(date=dt.date.today(), offset_n=0, list_out=False):
    """
    获取偏移交易日
    :param list_out: True输出list
    :param offset_n: 日期偏移量
    :param date: 输入日期
    :return:
    """
    d = pd.to_datetime(date).date()
    if offset_n <= 0:  # 前推
        sql_cmd = f"select top {-offset_n + 1} VTRADE_DATE from QT_TRADE_DATE where 1=1 " \
                  f"and EXCHANGE='001001' and IS_TRADE_DATE=1 and " \
                  f"TRADE_DATE<='{d}' order by TRADE_DATE desc"
        if not list_out:
            res = pd.read_sql_query(sql_cmd, conn1)['VTRADE_DATE'].to_list()[-1]
        else:
            res = pd.read_sql_query(sql_cmd, conn1)['VTRADE_DATE'].to_list()[:-1]

    elif offset_n > 0:  # 后推
        sql_cmd = f"select top {offset_n} NTRADE_DATE from QT_TRADE_DATE where 1=1 " \
                  f"and EXCHANGE='001001' and IS_TRADE_DATE=1 and " \
                  f"TRADE_DATE>='{d}' order by TRADE_DATE asc "
        if not list_out:
            res = pd.read_sql_query(sql_cmd, conn1)['NTRADE_DATE'].to_list()[-1]
        else:
            res = pd.read_sql_query(sql_cmd, conn1)['NTRADE_DATE'].to_list()
    return res


def get_stk_indus(stk_code_list: list = None, industry_name_list: list = None, date=str(dt.date.today()),
                  level: list or int = 1,
                  drop_new=True, drop_st=True):
    """
    获取股票所属中信行业
    :param drop_st: 去除ST和退市
    :param drop_new: 去除近一年上市新股
    :param industry_name_list:
    :param stk_code_list: 股票代码列表，默认None输出全部A股
    :param date: 日期
    :param level: 一 二 三
    :return:
    """
    ''''''
    date = str(pd.to_datetime(date).date())
    sql_cmd = f"select stock_code, stock_name, standard_name,industry_name, into_date, out_date, industry_code, industry_level " \
              f"from qt_indus_constituents where 1=1 and standard_code in (905, 906) " \
              f"and into_date <= '{date}' " \
              f"and (out_date >= '{date}' or out_date is null) " \
              f"order by stock_code"

    data_raw = pd.read_sql_query(sql_cmd, conn1)
    indus_pivot = data_raw.pivot(index='stock_code', columns='standard_name', values='industry_name').reset_index()
    data = indus_pivot.merge(data_raw[['stock_code', 'stock_name']].drop_duplicates(), how='left', on='stock_code')
    data = data[['stock_code', 'stock_name', '中信证券一级行业', '中信证券二级行业', '中信证券三级行业']]

    level_col = []
    if type(level) == int:
        level_col = ['中信证券' + ['占位', '一', '二', '三'][level] + '级行业']
        if industry_name_list:
            data = data[data[level_col[0]].isin(industry_name_list)]
    else:
        for l in level:
            level_name = '中信证券' + ['占位', '一', '二', '三'][l] + '级行业'
            level_col.append(level_name)

    if stk_code_list is not None:
        data = data[data['stock_code'].isin(stk_code_list)]

    data = data[['stock_code', 'stock_name'] + level_col]

    if drop_new:
        sql_cmd = "select stock_code,list_date from bas_stk_information"
        bas_info = pd.read_sql_query(sql_cmd, conn1).sort_values(by='list_date', ascending=False) \
            .drop_duplicates('stock_code')
        data = data.merge(bas_info, on='stock_code', how='left')
        data = data[data['list_date'] < str(dt.date.fromisoformat(date) - dt.timedelta(days=365))]
        data.pop('list_date')

    if drop_st:
        data = data[~(data['stock_name'].str.contains('ST') | data['stock_name'].str.contains('退'))]

    data = data.sort_values(by=['stock_code']).reset_index(drop=True)

    return data


def is_listed(stk_code: str, date=str(dt.date.today())):
    """
    判断股票在指定日期是否已上市
    :param stk_code: 单只股票代码
    :param date: 默认今日
    :return: True/False
    """
    sql_cmd = "select stock_code,list_date from bas_stk_information"
    bas_info = pd.read_sql_query(sql_cmd, conn1).sort_values(by='list_date', ascending=False) \
        .drop_duplicates('stock_code').set_index('stock_code')

    if bas_info['list_date'].loc[stk_code] > pd.to_datetime(date):
        return False
    else:
        return True


def is_st(stk_code: str, date=str(dt.date.today())):
    """
    判断股票在指定日期是否ST
    :param stk_code: 单只股票代码
    :param date: 默认今日的前一交易日
    :return: True/False
    """
    date = vtrade_date(date, -1)
    sql_cmd = f"select stock_code,stock_name " \
              f"from qt_stk_daily where 1=1 " \
              f"and trade_date = '{date}' "
    bas_info = pd.read_sql_query(sql_cmd, conn1).set_index('stock_code')
    if 'ST' in bas_info['stock_name'].loc[stk_code]:
        return True
    else:
        return False


def get_stk_price(stk_code_list: list, start_date=dt.date.today() - dt.timedelta(days=365),
                  end_date=str(dt.date.today())):
    """
    获取股价时间序列
    :param stk_code_list:
    :param start_date: isoformat
    :param end_date:
    :return:
    """

    stk_code_str = "'" + "','".join(stk_code_list) + "'"

    sql_cmd = f"select stock_code,trade_date,tclose,backward_adjratio,matiply_ratio " \
              f"from qt_stk_daily where 1=1 " \
              f"and stock_code in ({stk_code_str}) " \
              f"and trade_date >= '{start_date}' " \
              f"and trade_date<='{end_date}'"

    price_data = pd.read_sql_query(sql_cmd, conn1).sort_values(by=['stock_code', 'trade_date'])
    price_data_all = pd.DataFrame()

    for stk in stk_code_list:
        stk_df = price_data[price_data['stock_code'] == stk]
        r = stk_df['matiply_ratio'].iloc[-1]
        stk_df = stk_df.assign(close=lambda x: x['matiply_ratio'] / r * x['tclose'])
        stk_df = stk_df[['stock_code', 'trade_date', 'close']]
        price_data_all = pd.concat([price_data_all, stk_df], axis=0)

    res = price_data_all.pivot(index='trade_date', columns='stock_code', values='close')

    return res


def get_idx_price(idx_list: list = None, start_date: str = str(dt.date.today() - dt.timedelta(days=365)),
                  end_date: str = str(dt.date.today()), method=None, indus_df=None):
    """
    获取中信一级行业价格时间序列/区间涨跌幅
    :param indus_df:
    :param method:
    :param idx_list: 中信一级行业名称or代码
    :param start_date: 起始日
    :param end_date: 结束日，默认今天
    :return:
    """
    if not idx_list:
        idx_list = pd.read_sql_query("select distinct industry_code from citic_industry_index", conn2)['industry_code'] \
            .to_list()

    idx_code_str = "'" + "','".join(idx_list) + "'"
    idx_name_str = "'" + "','".join(idx_list) + "'"

    sql_cmd = f"select industry_code,industry_name, trade_date,tclose " \
              f"from citic_industry_index where 1=1 " \
              f"and (industry_code in ({idx_code_str}) or industry_name in ({idx_name_str})) " \
              f"and trade_date >= '{start_date}' " \
              f"and trade_date<='{end_date}'"

    data = pd.read_sql_query(sql_cmd, conn2)
    res = data.pivot(index='trade_date', columns='industry_name', values='tclose')

    # if args is None:
    if method == 'int_pct':
        res = (res - res.iloc[0, :]) / res.iloc[0, :]
        res = res.iloc[-1].to_frame()
        res.columns = ['区间涨跌幅']
        res = res.sort_values(by='区间涨跌幅', ascending=False)
    elif method == 'rank':
        res = (res - res.iloc[0, :]) / res.iloc[0, :]
        res = res.iloc[-1].rank(ascending=False).astype(int,errors='ignore').to_frame()
        res.columns = ['区间涨跌幅排名']
        res = res.sort_values(by='区间涨跌幅排名')
    elif method == 'calc' and indus_df.shape[1] == 1:
        indus_df.columns = ['排名']
        res = (res - res.iloc[0, :]) / res.iloc[0, :]
        res = res.iloc[-1].to_frame()
        res.columns = ['区间涨跌幅']
        indus_df = indus_df.merge(res, left_index=True, right_index=True, how='left').sort_values(by='排名')
        indus_df.loc[:, '区间涨跌幅排名'] = indus_df.loc[:, '区间涨跌幅'].rank(ascending=False).astype(int,errors='ignore')
        corr = indus_df[['排名', '区间涨跌幅排名']].corr('spearman')
        res = [indus_df, corr]
    return res


# def indus_backtest(indus_rank_df, start_date: str=str(dt.date.today()),ob_days:int=365):

