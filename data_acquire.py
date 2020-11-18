import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import datetime
import math
from collections import defaultdict
from copy import copy
import os

os.chdir(r'D:\Work\NMFintech\Test\Test')


def set_bond_type_trade_data(id):
    if id[2:4] == '00' or id[2:4] == '99':
        return 'TB'
    elif id[2:4] == '02' or id[2:4] == '77':
        return 'CDB'
    else:
        return 'PFB'




def set_0_in_same_time_bucket(x):
    for i in range(x.shape[0] - 1):
        if x.iloc[i, 2] == 5502 and x.iloc[i + 1, 2] == 5502:
            x.iloc[i, 2] = 0


def get_time_diff(x):
    for i in range(1, len(x.index)):
        if x.iloc[i, 7] == x.iloc[i - 1, 7]:
            x.iloc[i, 8] = x.iloc[i, 0] - x.iloc[i - 1, 0]
        else:
            x.iloc[i, 8] = x.iloc[i, 0] - datetime.datetime.combine(x.iloc[i, 7], datetime.time(6, 52, 0))

    for i in range(1, len(x.index)):
        x.iloc[i, 8] = x.iloc[i, 8].total_seconds()


def calculate_insert(x):
    x.insert(x.shape[1], 'yield_change', 0)
    x.insert(x.shape[1], 'date', 0)
    x.insert(x.shape[1], 'time_diff', 0)


def get_date_part(x):
    return x.date()


def Java_time_to_datetime(s):
    date = s[0:10]
    time = s[11:]
    if len(time) == 8:
        new = datetime.datetime.strptime(date + time, '%Y-%m-%d%H:%M:%S')
    elif len(time) == 5:
        new = datetime.datetime.strptime(date + time, '%Y-%m-%d%H:%M')
    elif len(time) == 2:
        new = datetime.datatime.strptime(date + time, '%Y-%m-%d%H')
    return new



def adjust_utf8(x):
    try:
        float(x['basis'])
    except ValueError:
        return float(x['theoretical yield'])
    else:
        return float(x['carryDate'])




def set_theo_data(path1,path2, data_id, test_bond_type, startTime  = datetime.datetime(year=2000,month=1,day=1), endTime = datetime.datetime(year = 2100,month=1,day=1)):
    if not path2:
        source_data = pd.read_csv(path1,
                #names=['fitTime', 'id', 'maturityDate', 'a', 'b', 'c', 'd', 'e', 'f', 'issueDate', 'yield', 'h', 'i'],
                engine='python')
    else:
        source_data1 = pd.read_csv(path1,
                #names=['fitTime', 'id', 'maturityDate', 'a', 'b', 'c', 'd', 'e', 'f', 'issueDate', 'yield', 'h', 'i'],
                engine='python')
        source_data2 = pd.read_csv(path2,
                #names=['fitTime', 'id', 'maturityDate', 'a', 'b', 'c', 'd', 'e', 'f', 'issueDate', 'yield', 'h', 'i'],
                engine='python')
        source_data = pd.concat([source_data1,source_data2])



    source_data['yield'] = source_data.apply(lambda x: adjust_utf8(x), axis=1)

    source_data = source_data[['time', 'bondId', 'maturityDate', 'yield']]
    source_data = source_data.rename(columns={'time': 'fitTime', 'bondId': 'id'})
    source_data.insert(loc=2, column='TYPE', value='theo')
    source_data.insert(loc=3, column='bond_type', value=test_bond_type)

    # normalize source_data
    source_data['fitTime'] = source_data['fitTime'].apply(lambda x: Java_time_to_datetime(x))
    source_data['maturityDate'] = source_data['maturityDate'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))

    # date from 2020-01-02 to 2020-06-05
    source_data = source_data[source_data['fitTime']>startTime]
    source_data = source_data[source_data['fitTime']<endTime]

    return source_data

def set_trade_data(path, data_id, test_bond_type, startTime  = datetime.datetime(year=2000,month=1,day=1), endTime = datetime.datetime(year = 2100,month=1,day=1)):
    # import trade_data
    trade_data = pd.read_excel(path)


    # normalize trade_data
    trade_data.rename(columns={'time': 'fitTime', 'trade_id': 'id', 'MD_entry_px': 'yield', 'maturity_date': 'maturityDate'},
    inplace=True)
    trade_data = trade_data[['fitTime', 'id', 'TYPE', 'bond_type', 'maturityDate', 'yield']]
    trade_data['bond_type'] = trade_data['id'].apply(lambda x: set_bond_type_trade_data(x))
    trade_data['yield'] /= 100
    trade_data = trade_data[trade_data['bond_type'] == test_bond_type]

    # trade_data time limit
    trade_data = trade_data[trade_data['fitTime'] < endTime]
    trade_data = trade_data[trade_data['fitTime'] > startTime]
    return trade_data

def set_bond_each_day(liquid_path, comparison_path, data_id,\
        test_bond_type, startTime  = datetime.datetime(year=2000,month=1,day=1), endTime = datetime.datetime(year = 2100,month=1,day=1)):

    liquidity_table = pd.read_csv(liquid_path)
    os.makedirs('Test_Result/'+data_id,exist_ok= True)


    try:
        datetime.datetime.strptime(liquidity_table['date'][0], '%Y/%m/%d')
    except ValueError:
        for i in range(len(liquidity_table['date'])):
            liquidity_table.loc[i, 'date'] = datetime.datetime.strptime(liquidity_table['date'][i], '%m/%d/%Y').date()
        liquid_dict = defaultdict(list)
        j = 1
        for i in range(len(liquidity_table['date'])):
            j = 1
            while j < liquidity_table.shape[1] and liquidity_table.iloc[i, j] != np.nan:
                liquid_dict[liquidity_table.loc[i, 'date']].append(liquidity_table.iloc[i, j])
                j += 1
    else:
          for i in range(0,len(liquidity_table['date'])):
              liquidity_table.loc[i, 'date'] = datetime.datetime.strptime(liquidity_table['date'][i], '%Y/%m/%d').date()
          liquid_dict = defaultdict(list)
          j = 3
          for i in range(len(liquidity_table['date'])):
              j = 3
              while j < liquidity_table.shape[1] and liquidity_table.iloc[i, j] != np.nan:
                  liquid_dict[liquidity_table.loc[i, 'date']].append(liquidity_table.iloc[i, j])
                  j += 1


    '''
    for i in liquid_dict:
        if len(liquid_dict[i]) != 0:
            for j in liquid_dict[i][0].split(','):
                liquid_dict[i].append(j)
    '''


    haks = pd.read_csv(comparison_path)


    haks['date'] = haks['fitTime'].apply(lambda x: Java_time_to_datetime(x))
    haks['date'] = haks['date'].apply(lambda x:x.date())

    try:
        datetime.datetime.strptime(haks['maturityDate'][i], '%Y-%m-%d').date()
    except ValueError:
        for i in range(len(haks['maturityDate'])):
            haks.loc[i, 'maturityDate'] = datetime.datetime.strptime(haks['maturityDate'][i], '%Y/%m/%d').date()
    else:
        for i in range(len(haks['maturityDate'])):
            haks.loc[i, 'maturityDate'] = datetime.datetime.strptime(haks['maturityDate'][i], '%Y-%m-%d').date()


    haks['days_to_maturity'] = haks['maturityDate'] - haks['date']
    haks['days_to_maturity'] = (haks['days_to_maturity'] / datetime.timedelta(days=1))
    haks['years_to_maturity'] = haks['days_to_maturity'] / 365.25

    haks['bond_type'] = test_bond_type

    def generate_maturity_bucket(data):
        for i in range(data.shape[0]):
            if data.loc[i, 'years_to_maturity'] <= 1:
                data.loc[i, 'maturity_type'] = 'short'
            elif 1 < data.loc[i, 'years_to_maturity'] <= 5:
                data.loc[i, 'maturity_type'] = 'mid'
            elif 5 < data.loc[i, 'years_to_maturity'] <= 10:
                data.loc[i, 'maturity_type'] = 'midlong'
            elif 10 < data.loc[i, 'years_to_maturity']:
                data.loc[i, 'maturity_type'] = 'long'

    haks['maturity_type'] = 0
    generate_maturity_bucket(haks)

    def generate_liquid_type(series):
        if series['id'] in liquid_dict[series['date']]:
            return 'high'
        else:
            return 'low'

    haks['liquidity_type'] = 0
    haks['liquidity_type'] = haks.apply(generate_liquid_type, axis=1)

    haks['coupon_type'] = haks['coupon'].apply(lambda x: 'high' if x >= 0.04 else 'low')

    haks

    # take 2020-01-02 to 2020-03-31
    haks = haks[haks['date'] < endTime.date()]
    haks = haks[haks['date'] > startTime.date()]


    haks['fitting_error'] = (haks['yieldSC'] - haks['yieldActual'])

    nmaturities = 4
    ncoupons = 2
    nliquidities = 2
    nbuckets = nmaturities * ncoupons * nliquidities
    maturity_list = ['short', 'mid', 'midlong', 'long']
    coupon_list = ['low', 'high']
    liquidity_list = ['low', 'high']

    class Test:
        def __init__(self, test_type, error_type, type_of_bond='TB', type_of_maturity='all',
                     type_of_coupon='all', type_of_liquidity='all'):
            self.test_type = test_type
            self.error_type = error_type
            self.bond_type = type_of_bond
            self.maturity_type = type_of_maturity
            self.coupon_type = type_of_coupon
            self.liquidity_type = type_of_liquidity
            if test_type == 'FE':
                self.data = haks

        def set_data(self, data):
            self.data = data

        def get_stats(self):
            bucket = Buckets(self.data)
            bucket_data = bucket.get_bucket(self.maturity_type, self.coupon_type, self.liquidity_type)
            if self.test_type == 'FE':
                if self.error_type == 'MAE':
                    return MAE(bucket_data).get_stats()


            elif self.test == 'PE':
                if self.error_type == 'MPE':
                    return MPE(bucket_data).get_stats()

    class Buckets:
        def __init__(self, data):
            self.data = data

        def get_bucket(self, type_of_maturity, type_of_coupon, type_of_liquidity):

            self.type_of_maturity = type_of_maturity
            self.type_of_coupon = type_of_coupon
            self.type_of_liquidity = type_of_liquidity

            # maturity
            if self.type_of_maturity == 'all':
                pass
            elif self.type_of_maturity == 'short':
                self.data = self.data[self.data['years_to_maturity'] <= 1]
            elif self.type_of_maturity == 'mid':
                self.data = self.data[(1 < self.data['years_to_maturity']) & (self.data['years_to_maturity'] <= 5)]
            elif self.type_of_maturity == 'midlong':
                self.data = self.data[(5 < self.data['years_to_maturity']) & (self.data['years_to_maturity'] <= 10)]
            else:
                self.data = self.data[self.data['years_to_maturity'] > 10]

            # coupon
            if self.type_of_coupon == 'all':
                pass
            elif self.type_of_coupon == 'low':
                self.data = self.data[self.data['coupon'] < 0.04]
            else:
                self.data = self.data[self.data['coupon'] >= 0.04]

            # liquidity
            if self.type_of_liquidity == 'all':
                pass
            elif self.type_of_liquidity == 'low':
                self.data = self.data[self.data['liquidity_type'] == 'low']
            elif self.type_of_liquidity == 'high':
                self.data = self.data[self.data['liquidity_type'] == 'high']

            return self.data

    class MPE:
        def __init__(self, data):
            self.data = data

        def get_stats(self):
            return

    class MAE:
        def __init__(self, data):
            self.data = data
            self.FE = data['fitting_error']
            self.judge = True
            if len(self.data['fitting_error']) == 0:
                self.judge = False

        def get_stats(self):
            if not self.judge:
                return pd.Series({'count': 0, 'Avg': 'N/A', 'Max': 'N/A', 'Min': 'N/A', '5% percentile':
                    'N/A', '95 percentile': 'N/A', 'Abs_Avg': 'N/A', 'Abs_Max': 'N/A', 'Abs_95% percentile': 'N/A'})
            return pd.Series({'count': len(self.FE), 'Avg': np.mean(self.FE), 'Max': max(self.FE), 'Min': min(self.FE),
                              '5% percentile': np.percentile(self.FE, 5), '95% percentile': np.percentile(self.FE, 95),
                              'Abs_Avg': np.mean(abs(self.FE)), 'Abs_Max': max(abs(self.data['fitting_error']))
                                 , 'Abs_95% percentile': np.percentile(abs(self.FE), 95)})

    '''    
    class RMSE:
        def __init__(self,data):
            self.data = data
            self.FE = data['fitting_error']**2
            self.judge = True
            if len(self.data['fitting_error']) ==0:
                self.judge = False

        def get_stats(self):
            if not self.judge:
                return pd.Series({'count':0,'Avg':'N/A','Max':'N/A','Min':'N/A','5% percentile':
                                 'N/A', '95 percentile':'N/A','Abs_Avg':'N/A','Abs_Max':'N/A','Abs_95% percentile':'N/A'})
            return pd.Series({'count':len(self.FE),'Avg':(np.mean(self.FE))**(1/2),'Max':(max(self.FE)),'Min':min(self.FE),
                              '5% percentile':np.percentile(self.FE,5),'95% percentile':np.percentile(self.FE,95),
                              'Abs_Avg':np.mean(abs(self.FE)),'Abs_Max':max(abs(self.data['fitting_error']))
                              ,'Abs_95% percentile':np.percentile(abs(self.FE),95)})
        '''

    FE_MAE = pd.DataFrame(
        columns=['count', 'Avg', 'Max', 'Min', '5% percentile', '95% percentile', 'Abs_Avg', 'Abs_Max',
                 'Abs_95% percentile'])
    for i in maturity_list:
        for j in coupon_list:
            for k in liquidity_list:
                temp = Test(test_type='FE', error_type='MAE', type_of_bond=test_bond_type, type_of_maturity=i,
                            type_of_coupon=j, type_of_liquidity=k).get_stats()

                FE_MAE.loc['FE_MAE_' + test_bond_type + '_' + i + '_' + j + '_' + k] = temp

    # latest version tb
    # FE_MAE.to_csv('Test_Result/20201015d.tb/FE_MAE_bond_maturity_coupon_liquidity.csv')
    # ===================================================================================
    # old version tb
    # FE_MAE.to_csv('Test_Result/' + data_id + '/FE_MAE_bond_maturity_coupon_liquidity_' + data_id + '.csv')

    bond_type_each_day = haks[['id', 'date', 'maturity_type', 'liquidity_type']]

    bond_type_each_day = bond_type_each_day.drop_duplicates(keep='first')
    bond_type_each_day = bond_type_each_day.reset_index(drop=True)

    bond_type_each_day.to_csv('data/' + data_id + '/bond_type_each_day_' + data_id + '.csv', index=False)






