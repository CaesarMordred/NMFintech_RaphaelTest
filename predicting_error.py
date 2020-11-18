import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
import math
import os

os.chdir(r'D:\Work\NMFintech\Test\Test')


def cal_PE(theo_data, real_data, data_id, test_bond_type):
    source_data = theo_data

    # import trade_data
    trade_data = real_data

    # encoding = 'GB2312'


    def set_0_in_same_time_bucket(x):
        for i in range(x.shape[0] - 1):
            if x.iloc[i, 2] == 5502 and x.iloc[i + 1, 2] == 5502:
                x.iloc[i, 2] = 0

    def get_date_part(x):
        return x.date()

    bond_catagory = pd.read_csv('data/' + data_id + '/bond_type_each_day_' + data_id + '.csv')

    bond_catagory['date'] = bond_catagory['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    bond_catagory['date'] = bond_catagory['date'].apply(lambda x: get_date_part(x))

    predicting_error_table = pd.DataFrame(
        columns=['fitTime', 'id', 'TYPE', 'bond_type', 'maturityDate', 'yield', 'real', 'theo', 'predicting_error',
                 'real_error', 'date', 'maturity_type', 'liquidity_type'])

    selected_bond_list = trade_data['id'].unique().tolist()
    for bond_id in selected_bond_list:

        new_source_data = source_data[source_data['id'] == bond_id]
        new_trade_data = trade_data[trade_data['id'] == bond_id]

        # concat
        merged_table = pd.concat([new_source_data, new_trade_data])
        merged_table = merged_table.sort_values(by=['fitTime', 'TYPE'])

        merged_table.reset_index(drop=True)

        set_0_in_same_time_bucket(merged_table)

        merged_table['TYPE'] = merged_table['TYPE'].replace(0, np.nan)
        merged_table = merged_table.dropna()

        merged_table = merged_table.reset_index(drop=True)

        merged_table['real'] = 0
        merged_table['theo'] = 0
        merged_table['predicting_error'] = 0

        if merged_table.iloc[0, 2] == 5502:
            merged_table.iloc[0, 6] = merged_table.iloc[0, 5]
        else:
            pass

        for i in range(1, merged_table.shape[0]):
            if merged_table.iloc[i, 2] == 5502 and merged_table.iloc[i - 1, 2] == 'theo':
                merged_table.iloc[i, 6] = merged_table.iloc[i, 5]
                merged_table.iloc[i, 7] = merged_table.iloc[i - 1, 5]
                merged_table.iloc[i, 8] = merged_table.iloc[i, 7] - merged_table.iloc[i, 6]

        merged_table['real'] = merged_table['real'].replace(0, np.nan)
        merged_table = merged_table.dropna()

        merged_table = merged_table.reset_index(drop=True)

        merged_table['real_error'] = merged_table['real'].diff()
        merged_table = merged_table.dropna()
        merged_table['date'] = merged_table['fitTime'].apply(lambda x: get_date_part(x))
        merged_table = pd.merge(merged_table, bond_catagory, on=['id', 'date'])

        predicting_error_table = pd.concat([predicting_error_table, merged_table])

    predicting_error_table = predicting_error_table.sort_values(by=['fitTime'])

    predicting_error_table = predicting_error_table.reset_index(drop=True)

    maturity_list = ['short', 'mid', 'midlong', 'long']
    liquidity_list = ['low', 'high']

    PE_MPE = pd.DataFrame(
        columns=['count', 'Avg(theo-real)', 'Max', 'Min', '5% percentile', '95% percentile', 'Abs_Avg', 'Abs_Max',
                 'Abs_95% percentile'])
    PE_RE = pd.DataFrame(
        columns=['count', 'Avg(theo-real)', 'Max', 'Min', '5% percentile', '95% percentile', 'Abs_Avg', 'Abs_Max',
                 'Abs_95% percentile'])

    for i in maturity_list:
        for j in liquidity_list:
            bond_data_final = predicting_error_table[predicting_error_table['maturity_type'] == i]
            bond_data_final = bond_data_final[bond_data_final['liquidity_type'] == j]
            PE = bond_data_final['predicting_error']
            RE = bond_data_final['real_error']

            if len(bond_data_final) == 0:
                PE_MPE.loc['PE_MPE_' + test_bond_type + '_' + i + '_' + j] = pd.Series(
                    {'count': 0, 'Avg(theo-real)': 'N/A', 'Max': 'N/A', 'Min': 'N/A', '5% percentile':
                        'N/A', '95 percentile': 'N/A', 'Abs_Avg': 'N/A', 'Abs_Max': 'N/A', 'Abs_95% percentile': 'N/A'})
                PE_RE.loc['RE_' + test_bond_type + '_' + i + '_' + j] = pd.Series(
                    {'count': 0, 'Avg(theo-real)': 'N/A', 'Max': 'N/A', 'Min': 'N/A', '5% percentile':
                        'N/A', '95 percentile': 'N/A', 'Abs_Avg': 'N/A', 'Abs_Max': 'N/A', 'Abs_95% percentile': 'N/A'})
            else:
                MPE = pd.Series({'count': len(PE), 'Avg(theo-real)': np.nanmean(PE), 'Max': max(PE), 'Min': min(PE),
                                 '5% percentile': np.percentile(PE, 5), '95% percentile': np.percentile(PE, 95),
                                 'Abs_Avg': np.mean(abs(PE)), 'Abs_Max': max(abs(PE))
                                    , 'Abs_95% percentile': np.percentile(abs(PE), 95)})
                RPE = pd.Series({'count': len(RE), 'Avg(theo-real)': np.nanmean(RE), 'Max': max(RE), 'Min': min(RE),
                                 '5% percentile': np.percentile(RE, 5), '95% percentile': np.percentile(RE, 95),
                                 'Abs_Avg': np.mean(abs(RE)), 'Abs_Max': max(abs(RE))
                                    , 'Abs_95% percentile': np.percentile(abs(RE), 95)})
                PE_MPE.loc['PE_MPE_' + test_bond_type + '_' + i + '_' + j] = MPE
                PE_RE.loc['RE_' + test_bond_type + '_' + i + '_' + j] = RPE

    PE_MPE.to_csv('Test_Result/' + data_id + '/PE_MPE_bond_maturity_liquidity_' + data_id + '.csv')

    PE_RE.to_csv('Test_Result/' + data_id + '/PE_RE_bond_maturity_liquidity_' + data_id + '.csv')

    return PE_MPE
