import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import math
import data_acquire
import os

os.chdir(r'D:\Work\NMFintech\Test\Test')


def cal_var_vol(theo_data, real_data, data_id, test_bond_type):

    source_data = theo_data


    # import trade_data
    trade_data = real_data


    # encoding = 'GB2312'

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



    day_list = trade_data['fitTime'].apply(lambda x: datetime.datetime.date(x)).unique().tolist()

    drop_bond_list = []

    real_var_table = pd.DataFrame(columns=day_list)
    real_vol_table = pd.DataFrame(columns=day_list)
    theo_var_table = pd.DataFrame(columns=day_list)
    theo_vol_table = pd.DataFrame(columns=day_list)

    # select bond id
    selected_bond_list = trade_data['id'].unique().tolist()
    for bond_id in selected_bond_list:
        new_source_data = source_data[source_data['id'] == bond_id]
        new_trade_data = trade_data[trade_data['id'] == bond_id]

        # concat
        merged_table = pd.concat([new_source_data, new_trade_data])
        merged_table = merged_table.sort_values(by=['fitTime', 'TYPE'])

        merged_table.reset_index(drop=True)

        set_0_in_same_time_bucket(merged_table)
        merged_table

        merged_table['TYPE'] = merged_table['TYPE'].replace(0, np.nan)
        merged_table.dropna()

        new_theo = merged_table[merged_table['TYPE'] == 'theo']
        new_real = merged_table[merged_table['TYPE'] == 5502]

        new_theo = new_theo.reset_index(drop=True)
        new_real = new_real.reset_index(drop=True)

        for case in range(2):
            if case == 0:
                _data = new_real
            else:
                _data = new_theo

            calculate_insert(_data)
            _data['date'] = _data['fitTime'].apply(lambda x: get_date_part(x))
            _data['yield'] *= 100
            _data['yield_change'] = _data['yield'].diff()
            get_time_diff(_data)

            if len(_data) < 2:
                drop_bond_list.append(bond_id)
                continue

            _data = _data.drop(index=0)

            _data = _data.reset_index(drop=True)
            _data['yield_change_square'] = np.square(_data['yield_change'])
            _count = _data.groupby('date')['yield_change'].count()
            _count[_count < 2] = np.nan
            _data['part_1'] = _data['yield_change_square'] / _data['time_diff']
            front_part = _data.groupby('date')['part_1'].sum()
            back_part = (np.square(_data.groupby('date')['yield_change'].sum())) / (
                _data.groupby('date')['time_diff'].sum())
            _var = (front_part - back_part) / _count
            _var.name = bond_id
            _vol = _var ** (1 / 2)
            _vol.name = bond_id

            if case == 0:
                real_var_table = real_var_table.append(_var)
                real_vol_table = real_vol_table.append(_vol)
            else:
                theo_var_table = theo_var_table.append(_var)
                theo_vol_table = theo_vol_table.append(_vol)

    real_var_table.to_csv('Test_Result/' + data_id + '/real_var_table_' + data_id + '.csv')

    theo_vol_table.to_csv('Test_Result/' + data_id + '/theo_vol_table_' + data_id + '.csv')

    theo_var_table.to_csv('Test_Result/' + data_id + '/theo_var_table_' + data_id + '.csv')

    real_vol_table.to_csv('Test_Result/' + data_id + '/real_vol_table_' + data_id + '.csv')


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


    bond_catagory = pd.read_csv('data/' + data_id + '/bond_type_each_day_' + data_id + '.csv')

    bond_catagory

    maturity_list = ['short', 'mid', 'midlong', 'long']
    liquidity_list = ['low', 'high']
    Var_Vol_Test = pd.DataFrame(columns=['count', 'real_var', 'real_vol', 'theo_var', 'theo_vol', 'ratio_var', 'ratio_vol'])

    debug_real_var_bucket_list = pd.DataFrame(columns=['date', 'id', 'value', 'maturity_type', 'liquidity_type'])
    debug_real_vol_bucket_list = pd.DataFrame(columns=['date', 'id', 'value', 'maturity_type', 'liquidity_type'])
    debug_theo_var_bucket_list = pd.DataFrame(columns=['date', 'id', 'value', 'maturity_type', 'liquidity_type'])
    debug_theo_vol_bucket_list = pd.DataFrame(columns=['date', 'id', 'value', 'maturity_type', 'liquidity_type'])

    for i in maturity_list:
        for j in liquidity_list:
            bond_date_list = bond_catagory[bond_catagory['maturity_type'] == i]
            bond_date_list = bond_date_list[bond_date_list['liquidity_type'] == j]

            real_var_bucket_list = []
            real_vol_bucket_list = []
            theo_var_bucket_list = []
            theo_vol_bucket_list = []

            bond_date_list['date'] = bond_date_list['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
            bond_date_list['date'] = bond_date_list['date'].apply(lambda x: get_date_part(x))
            bond_date_list = bond_date_list.reset_index(drop=True)

            bond_date_tuples = []

            for k in range(len(bond_date_list.index)):
                bond_date_tuples.append((bond_date_list.iloc[k, 0], bond_date_list.iloc[k, 1]))

            for particular_bond, particular_date in bond_date_tuples:

                try:
                    math.isnan(real_var_table[particular_date][particular_bond])
                except KeyError:
                    continue
                else:
                    pass

                if math.isnan(real_var_table[particular_date][particular_bond]):
                    continue
                else:
                    real_var_bucket_list.append(real_var_table[particular_date][particular_bond])
                    real_vol_bucket_list.append(real_vol_table[particular_date][particular_bond])
                    theo_var_bucket_list.append(theo_var_table[particular_date][particular_bond])
                    theo_vol_bucket_list.append(theo_vol_table[particular_date][particular_bond])

                    debug_real_var_bucket_list = debug_real_var_bucket_list.append(pd.Series(
                        {'date': particular_date, 'id': particular_bond,
                         'value': real_var_table[particular_date][particular_bond], 'maturity_type': i,
                         'liquidity_type': j}), ignore_index=True)
                    debug_real_vol_bucket_list = debug_real_vol_bucket_list.append(pd.Series(
                        {'date': particular_date, 'id': particular_bond,
                         'value': real_vol_table[particular_date][particular_bond], 'maturity_type': i,
                         'liquidity_type': j}), ignore_index=True)
                    debug_theo_var_bucket_list = debug_theo_var_bucket_list.append(pd.Series(
                        {'date': particular_date, 'id': particular_bond,
                         'value': theo_var_table[particular_date][particular_bond], 'maturity_type': i,
                         'liquidity_type': j}), ignore_index=True)
                    debug_theo_vol_bucket_list = debug_theo_vol_bucket_list.append(pd.Series(
                        {'date': particular_date, 'id': particular_bond,
                         'value': theo_vol_table[particular_date][particular_bond], 'maturity_type': i,
                         'liquidity_type': j}), ignore_index=True)

            real_var_mean = np.mean(real_var_bucket_list)
            theo_var_mean = np.mean(theo_var_bucket_list)

            bucket_stats = pd.Series(
                {'count': len(real_var_bucket_list), 'real_var': real_var_mean, 'real_vol': np.sqrt(real_var_mean)
                    , 'theo_var': theo_var_mean, 'theo_vol': np.sqrt(theo_var_mean),
                 'ratio_var': theo_var_mean / real_var_mean,
                 'ratio_vol': np.sqrt(theo_var_mean) / np.sqrt(real_var_mean)
                 })
            Var_Vol_Test.loc['maturity_type = ' + i + '   and   ' + 'liquidity_type = ' + j] = bucket_stats

    Var_Vol_Test.to_csv('Test_Result/' + data_id + '/Var_Vol_Test_result_' + data_id + '.csv')


''' 
    debug_real_var_bucket_list.to_csv('Test_Result/' + data_id + '/debug_real_var_bucket_list.csv')
    debug_real_vol_bucket_list.to_csv('Test_Result/' + data_id + '/debug_real_vol_bucket_list.csv')
    debug_theo_var_bucket_list.to_csv('Test_Result/' + data_id + '/debug_theo_var_bucket_list.csv')
    debug_theo_vol_bucket_list.to_csv('Test_Result/' + data_id + '/debug_theo_vol_bucket_list.csv')

'''

