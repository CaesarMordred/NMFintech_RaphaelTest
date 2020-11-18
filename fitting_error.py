import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime
from collections import defaultdict
import math
import os

# 路径，需要修改
os.chdir(r'D:\Work\NMFintech\Test\Test')

# 整个函数计算了FE，输入如下
def cal_FE(theo_data, real_data, data_id, test_bond_type):
    # source_data为理论数据
    source_data = theo_data

    # import trade_data
    trade_data = real_data

    # encoding = 'GB2312'
    # encoding是有时候会遇到的问题

    # 这个函数会用于将真实交易数据和理论数据连接排序之后，用于删除相同10分钟内的同只债券的交易，仅保留最后一笔
    def set_0_in_same_time_bucket(x):
        for i in range(x.shape[0] - 1):
            if x.iloc[i, 2] == 5502 and x.iloc[i + 1, 2] == 5502:
                x.iloc[i, 2] = 0

    # 用于求datetime格式的date数据
    def get_date_part(x):
        return x.date()

    # bond_category的获得，来源于生成的bond_type_each_day文件， catagory为typo
    bond_catagory = pd.read_csv('data/' + data_id + '/bond_type_each_day_' + data_id + '.csv')

    # 读取出来的文件都是str格式，要转化为datetime格式，再转换为date格式
    bond_catagory['date'] = bond_catagory['date'].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
    bond_catagory['date'] = bond_catagory['date'].apply(lambda x: get_date_part(x))

    # 这个table是之后我们会用来计算FE的
    fitting_error_table = pd.DataFrame(
        columns=['fitTime', 'id', 'TYPE', 'bond_type', 'maturityDate', 'yield', 'real', 'theo', 'fitting_error',
                 'real_error', 'date', 'maturity_type', 'liquidity_type'])

    # select_bond_list 是包含了所有交易数据的bond_id的list
    selected_bond_list = trade_data['id'].unique().tolist()

    # 对于每个bond我们需要单独拿出来计算
    for bond_id in selected_bond_list:

        # new_source_data 为单独一只债券的理论数据，new_trade_data 为其实际交易数据
        new_source_data = source_data[source_data['id'] == bond_id]
        new_trade_data = trade_data[trade_data['id'] == bond_id]

        # concat
        # 我们将两个数据表格连起来，并按照时间和TYPE同时排序，这里非常注意的地方在于一定要按照TYPE作为第二参照排序，
        # 否则在实际交易有多笔且和理论同时发生的场合会导致理论数据上下可能都有同时的实际交易数据，会导致之后出现错误。
        merged_table = pd.concat([new_source_data, new_trade_data])
        merged_table = merged_table.sort_values(by=['fitTime', 'TYPE'])

        # 重置index
        merged_table.reset_index(drop=True)

        # 将所有的同一bucket内想要舍弃的交易数据（非最后一笔）的TYPE全部改为0
        set_0_in_same_time_bucket(merged_table)

        # drop掉这些数据并重置index
        merged_table['TYPE'] = merged_table['TYPE'].replace(0, np.nan)
        merged_table = merged_table.dropna()
        merged_table = merged_table.reset_index(drop=True)

        # 初始化real，theo和fitting_error
        merged_table['real'] = 0
        merged_table['theo'] = 0
        merged_table['fitting_error'] = 0

        # 我们先考虑如果第一项为5502的话，我们希望可以保留他，所以我们会把他的yield赋给real这一列
        if merged_table.iloc[0, 2] == 5502:
            merged_table.iloc[0, 6] = merged_table.iloc[0, 5]
        else:
            pass

        # 如果这是一条5502，且下一条是theo，那么我们会保留他，并且把下一条的theo yield赋给这条5502的theo
        # 最后我们将只保留5502的数据，来确保FE数据的数量和实际交易数量一致

        # 具体阅读因为用了iloc函数，请记载下column name再对照阅读
        # 这里第八列以及有了FE，且不会出现na
        for i in range(merged_table.shape[0] - 1):
            if merged_table.iloc[i, 2] == 5502 and merged_table.iloc[i + 1, 2] == 'theo':
                merged_table.iloc[i, 6] = merged_table.iloc[i, 5]
                merged_table.iloc[i, 7] = merged_table.iloc[i + 1, 5]
                merged_table.iloc[i, 8] = merged_table.iloc[i, 7] - merged_table.iloc[i, 6]

        merged_table['real'] = merged_table['real'].replace(0, np.nan)
        merged_table = merged_table.dropna()
        merged_table = merged_table.reset_index(drop=True)

        # 这里求了real的diff()，如果之前保留了第一行，也会在这里删掉
        merged_table['real_error'] = merged_table['real'].diff()
        merged_table = merged_table.dropna()

        # 按照日期和id去合并来获得maturity和liquidity
        merged_table['date'] = merged_table['fitTime'].apply(lambda x: get_date_part(x))
        merged_table = pd.merge(merged_table, bond_catagory, on=['id', 'date'])

        fitting_error_table = pd.concat([fitting_error_table, merged_table])


    fitting_error_table = fitting_error_table.sort_values(by=['fitTime'])

    fitting_error_table = fitting_error_table.reset_index(drop=True)

    maturity_list = ['short', 'mid', 'midlong', 'long']
    liquidity_list = ['low', 'high']

    FE_MAE = pd.DataFrame(
        columns=['count', 'Avg(theo-real)', 'Max', 'Min', '5% percentile', '95% percentile', 'Abs_Avg', 'Abs_Max',
                 'Abs_95% percentile'])

    for i in maturity_list:
        for j in liquidity_list:
            bond_data_final = fitting_error_table[fitting_error_table['maturity_type'] == i]
            bond_data_final = bond_data_final[bond_data_final['liquidity_type'] == j]
            FE = bond_data_final['fitting_error']

            if len(bond_data_final) == 0:
                FE_MAE.loc['FE_MFE_' + test_bond_type + '_' + i + '_' + j] = pd.Series(
                    {'count': 0, 'Avg(theo-real)': 'N/A', 'Max': 'N/A', 'Min': 'N/A', '5% percentile':
                        'N/A', '95 percentile': 'N/A', 'Abs_Avg': 'N/A', 'Abs_Max': 'N/A', 'Abs_95% percentile': 'N/A'})
            else:
                MAE = pd.Series({'count': len(FE), 'Avg(theo-real)': np.nanmean(FE), 'Max': max(FE), 'Min': min(FE),
                                 '5% percentile': np.percentile(FE, 5), '95% percentile': np.percentile(FE, 95),
                                 'Abs_Avg': np.mean(abs(FE)), 'Abs_Max': max(abs(FE))
                                    , 'Abs_95% percentile': np.percentile(abs(FE), 95)})
                FE_MAE.loc['FE_MFE_' + test_bond_type + '_' + i + '_' + j] = MAE

    FE_MAE.to_csv('Test_Result/' + data_id + '/FE_MFE_bond_maturity_liquidity_' + data_id + '.csv')

    return FE_MAE
