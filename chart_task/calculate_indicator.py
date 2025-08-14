import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from datetime import datetime

'''
nav 为净值或价格的TS序列，类型为pd.Series
示例：
2006-03-03    1.000000
2006-03-06    0.996643
2006-03-07    0.974189
2006-03-08    0.966819
2006-03-09    0.962784
2006-03-10    0.963161
2006-03-13    0.974002
2006-03-14    0.973515
2006-03-15    0.985707
2006-03-16    0.985226
Name: 上证综指, dtype: float64

调用方式参考 最后的示例
'''

class ComputeIndicator:
    
    def __init__(self):
        self.freq_indicator = ['ewm_return_ratio', 'yearly_return', 'sharpe_ratio',
                               'downside_vol', 'sortino_ratio', 'vol', 'calmar_ratio'] #净值/价格频率有关的指标放入次list
        
        self.func_dict = {'adjust_nav': self.adjust_nav, #调整净值中的极值
                          'return_ratio': self.return_ratio, #总收益率
                          'year': self.calc_year, #数据年限
                          'annualized_return_ratio': self.annualized_return_ratio,#年化收益率
                          'ewm_return_ratio': self.ewm_return_ratio,#近一年EWM加权收益率
                          'yearly_return': self.yearly_return,#计算各年收益率，少于10个月不计算，今年除外
                          'max_drawdown': self.max_drawdown,#最大回撤
                          'max_drawdown_duration': self.max_drawdown_duration,#回撤最大天数
                          'hurst': self.hurst,#赫斯特指数
                          'new_high_index1': self.new_high_index1,#新高指数1:创新高为1，不创新高为0
                          'new_high_index2': self.new_high_index2,#新高指数2:创新高为1，不创新高为回撤幅度
                          'sharpe_ratio': self.sharpe_ratio,#夏普比率
                          'win_ratio': self.win_ratio,#胜率
                          'downside_vol': self.downside_vol,#下行波动率
                          'vol': self.vol,#波动率
                          'profit_loss_ratio': self.profit_loss_ratio,#盈亏比
                          'calmar_ratio': self.calmar_ratio,#卡马比率
                          'sortino_ratio': self.sortino_ratio,#索提诺比率
                          'off_last_high': self.off_last_high_days, #离前高天数
                          'period_return': self.period_return,#近*日/周/月/年收益率,格式为 *d, *w, *M, *y(*为数字，比如 2w 代表近两周)
                          }
    
    #调整净值
    def adjust_nav(self, nav):
        ts = nav.dropna().pct_change()
        if len(ts)>200:
            ts[ts>ts.quantile(0.99)] = ts.quantile(0.99)
            ts[ts<ts.quantile(0.01)] = ts.quantile(0.01)
        elif len(ts)>100:
            ts[ts>ts.quantile(0.98)] = ts.quantile(0.98)
            ts[ts<ts.quantile(0.02)] = ts.quantile(0.02)
        elif len(ts)>50:
            ts[ts>ts.quantile(0.97)] = ts.quantile(0.97)
            ts[ts<ts.quantile(0.03)] = ts.quantile(0.03)
        else:
            ts[ts>ts.quantile(0.96)] = ts.quantile(0.96)
            ts[ts<ts.quantile(0.04)] = ts.quantile(0.04)
            
        return (1 + ts).cumprod()
    
    #收益率
    def return_ratio(self, nav):
        nav = nav.dropna()
        return pd.Series([nav.iloc[-1] / nav.iloc[0] - 1], index=['总收益率'])
    
    def calc_year(self, nav):
        nav = nav.dropna()
        nav.sort_index(inplace=True)
        return pd.Series([(nav.index[-1] - nav.index[0]).days/365], index=['年限'])
    
    #年化收益率
    def annualized_return_ratio(self, nav):
        nav = nav.dropna()
        year = self.calc_year(nav).iloc[0]
        ret = nav.iloc[-1] / nav.iloc[0]
        return pd.Series([(ret**(1/year)) - 1], index=['年化收益率'])

    #近一年EWM加权收益率
    def ewm_return_ratio(self, nav, freq='d', halflife=9):
        nav = nav.dropna()
        ret = np.log(nav/nav.shift())
        if freq == 'd':
            num = 244
        elif freq == 'w':
            num = 52
        elif freq == 'm':
            num = 12
        elif freq == 'y':
            num = 1
        else:
            raise Exception('time series data frequency Error!')
        return pd.Series([ret[-num:].ewm(halflife=halflife).mean().dropna().iloc[-1]], index=['ewm_收益率'], name=nav.name)

    #计算各年收益率，少于10个月不计算，今年除外
    def yearly_return(self, nav, freq='d'):
        year = datetime.now().strftime('%Y') + '年'
        nav_pct = nav.dropna().pct_change().fillna(0)
        if freq == 'd':
            min_num = 122 #一年244个交易日， 减去两个月122个交易日
        elif freq == 'w':
            min_num = 26 #一年52周， 减去两个月26周
        elif freq == 'y':
            min_num = 1
        yearly_return = nav_pct.resample('y').apply(lambda x: (x + 1).prod() - 1 if len(x) > min_num else np.nan)
        yearly_return.index = yearly_return.index.strftime('%Y')
        yearly_return.dropna(how='all', inplace=True)
        yearly_return.index += '年'
        if year in yearly_return.index:
            yearly_return.index = yearly_return.index.map(lambda x: '今年以来' if x == year else x)
        else:
            this_year = (nav_pct[nav_pct.index.year.isin([year[:4]])] + 1).prod() - 1
            yearly_return['今年以来'] = this_year     
        yearly_return.index += '收益率'
        return yearly_return

    #离前高天数
    def off_last_high_days(self, nav):
        nav = nav.dropna()
        dd = nav/nav.cummax() - 1
        dd = dd[dd==0]
        gap_days = (nav.index[-1] - dd.index[-1]).days
        return pd.Series([gap_days], index=['离前高天数'])
    
    def max_drawdown(self, nav):
        nav = nav.dropna()
        mdd = (nav/nav.cummax()-1).abs().max()
        return pd.Series([mdd], index=['最大回撤'])

    #回撤最大天数
    def max_drawdown_duration(self, nav):
        nav = nav.dropna()
        dd = nav/nav.cummax() - 1
        dd[dd.index==dd.index.max()] = 0
        dd = dd[dd==0].to_frame().reset_index()
        dd.columns = ['drawdown_end_date', dd.columns[-1]]
        dd['drawdown_start_date'] = dd.drawdown_end_date.shift(1)
        dd['gap_days'] = (dd.drawdown_end_date - dd.drawdown_start_date).dt.days
        return pd.Series([dd.gap_days.max()], index=['最大回撤持续天数'])
    
    #赫斯特指数
    def hurst(self, nav):
        nav = nav.dropna()
        #HURST指数
        # Create the range of lag values
        lags = range(2, 100)
        # Calculate the array of the variances of the lagged differences
        tau = [np.sqrt(np.std(np.subtract(nav[lag:], nav[:-lag]))) for lag in lags]
        # Use a linear fit to estimate the Hurst Exponent
        poly = np.polyfit(np.log(lags), np.log(tau), 1)
        # Return the Hurst exponent from the polyfit output
        return pd.Series([poly[0]*2.0], index=['hurst'])

    #新高指数1
    def new_high_index1(self, nav, halflife=244):
        nav = nav.dropna()
        idxmax_ts = pd.Series(index=nav.index)
        for item in nav.index:
            idxmax_ts[item] = nav.loc[:item].idxmax()
        idxmax_ts = idxmax_ts.reset_index()
        idxmax_ts.columns = ['real_date', 'max_date']
        idxmax_ts['real_nav'] = [nav[q] for q in idxmax_ts.real_date.tolist()]
        idxmax_ts['max_nav'] = [nav[q] for q in idxmax_ts.max_date.tolist()]
        idxmax_ts['max_nav'] = idxmax_ts['max_nav'].shift()
        new_high = (idxmax_ts['real_nav']>=idxmax_ts['max_nav']).astype(int)
        new_high.iloc[0] = 1
#        new_high = new_high.iloc[-52:].ewm(halflife=9).mean().dropna().iloc[-1]
        new_high = new_high.ewm(halflife=halflife).mean().dropna()
        return pd.Series([new_high.iloc[-1]], index=['新高指数1'])
    
    #新高指数2
    def new_high_index2(self, nav, halflife=244): #nav 为 pandas.Series
        nav = nav.dropna()
        name = nav.name
        nav.dropna(inplace=True)
        nav = nav.to_frame()
        nav['cummax'] = nav[name]/nav[name].cummax()
        nav['new_high_index'] = nav['cummax'].ewm(halflife=halflife).mean()
        return pd.Series([nav.new_high_index.iloc[-1]], index=['新高指数2'])
        
    #夏普比率
    def sharpe_ratio(self, nav, rf=0.03, freq='d'):
        nav = nav.dropna()
        ar = self.annualized_return_ratio(nav).iloc[0]
        vol = self.vol(nav, freq=freq).iloc[0]
        return pd.Series([(ar - rf) / (vol + 1e-6)], index=['夏普比率'])

    # 胜率
    def win_ratio(self, nav):
        revenue = nav.dropna().pct_change()
        return pd.Series([(revenue >= 0).mean()], index=['胜率'])

    #下行波动率
    def downside_vol(self, nav, freq='d'):
        # 下行波动率
        nav = nav.dropna()
        revenue = nav.pct_change()
        nav_ds = (1 + revenue[revenue<0]).cumprod()
        return pd.Series([self.vol(nav_ds, freq=freq).iloc[0]], index=['下行波动率'])
    
    #波动率
    def vol(self, nav, freq='d'):
        # 下行波动率
        revenue = nav.dropna().pct_change()
        if freq == 'd':
            num = 244 
        elif freq == 'w':
            num = 52
        elif freq == 'y':
            num = 12
        return pd.Series([revenue.std() * (num ** 0.5)], index=['波动率'])

    #盈亏比
    def profit_loss_ratio(self, nav):
        # 盈亏比
        revenue = nav.dropna().pct_change()
        pl_ration = revenue[revenue>0].mean() / (revenue[revenue<0].abs().mean() + 1e-6)
        return pd.Series([pl_ration], index=['盈亏比'])

    #卡马
    def calmar_ratio(self, nav, freq='d'):
        nav = nav.dropna()
        ar = self.annualized_return_ratio(nav).iloc[0]
        mdd = self.max_drawdown(nav).iloc[0]
        return pd.Series([ar / (mdd + 1e-6)], index=['Calmar'])
    
    #索提诺比率
    def sortino_ratio(self, nav, rf=0.03, freq='d'):
        nav = nav.dropna()
        ann_return = self.annualized_return_ratio(nav).iloc[0]
        ds_vol = self.downside_vol(nav, freq=freq).iloc[0]
        
        return pd.Series([(ann_return - rf) / (ds_vol + 1e-6)], index=['索提诺比率'])
    
    #可计算近n天、近n周、近n月、近n年收益率
    def period_return(self, nav, period='1M'):
        nav = nav.dropna()
        period_split = list(period)
        num = ''.join(period_split[:-1])
        if period_split[-1] == 'd':
            time_type = '天'
            start_date = nav.index[-1] - relativedelta(days=int(num))
        elif period_split[-1] == 'w':
            time_type = '周'
            start_date = nav.index[-1] - relativedelta(weeks=int(num))
        elif period_split[-1] == 'M':
            time_type = '月'
            start_date = nav.index[-1] - relativedelta(months=int(num))
        elif period_split[-1] == 'y':
            time_type = '年'
            start_date = nav.index[-1] - relativedelta(years=int(num))
        else:
            raise Exception(f'date type error: {period}')

        # start_date = nav.index[-1] - pd.to_timedelta(int(num), unit=period_split[-1])
        
        period_name = '近' + num + time_type + '收益率'
        try:
            pre_nav = nav[:start_date].iloc[-1]
        except:
            return pd.Series([np.nan], index=[period_name])
        return pd.Series([nav.iloc[-1] / pre_nav - 1], index=[period_name])
    
    #多指标同时计算
    def multi_indicator(self, nav, indicator_list=['return_ratio', 'sortino_ratio'], freq='d'):
        
        df = pd.DataFrame()
        for ic in indicator_list:
            if ic in self.func_dict.keys():
                if ic in self.freq_indicator:
                    idf = self.func_dict[ic](nav, freq=freq)
                else:
                    idf = self.func_dict[ic](nav)
            elif ic[0].isdigit():
                idf = self.period_return(nav, period=ic)
            else:
                raise Exception(f'indicator error: {ic}')
            df = pd.concat([df ,idf])
        return df[0]


if __name__ == "__main__":
    
    #生成随机收益率序列
    data = [i[0] for i in np.random.randn(1000, 1)]
    index = [datetime.now() - pd.to_timedelta(i, unit='d') for i in range(1000)][::-1]
    nav = (pd.Series(data, index=index, name='random_data')/100 + 1).cumprod()
    nav.sort_index(inplace=True)
    nav_df = nav.to_frame()

    #创建ComputeIndicator类
    ci = ComputeIndicator()
    
#    #单个指标计算
#    #pd.Series 调用方式
#    indicator_1 = ci.return_ratio(nav)
#    indicator_1.name = nav.name
#    
#    indicator_2 = ci.multi_indicator(nav, indicator_list=['return_ratio'])
#    indicator_2.name = nav.name
#    
#    #pd.DataFrame 调用方式
#    indicator_3 = nav_df.apply(ci.return_ratio)
#    indicator_3.name = nav.name
#    
#    indicator_4 = nav_df.apply(ci.multi_indicator, indicator_list=['return_ratio'])
#    
#    #多指标计算
#    #pd.Series 调用方式
#    indicator_5 = ci.multi_indicator(nav, indicator_list=['return_ratio', 'sortino_ratio', '1y', 'yearly_return'])
#    indicator_5.name = nav.name
    
    #pd.DataFrame 调用方式
    indicator_list = ['annualized_return_ratio', 'max_drawdown', 'max_drawdown_duration', 'sharpe_ratio', 'win_ratio', 'downside_vol',
     'profit_loss_ratio', 'calmar_ratio', 'off_last_high']
    indicator_6 = nav_df.apply(ci.multi_indicator, indicator_list=indicator_list).T
    indicator_dict = indicator_6.to_dict(orient='index')

    print(indicator_dict)

