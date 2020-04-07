
# %% Initial Settings: Load Required Libraries
import os
import sys
import platform
import pandas as pd
import numpy as np
from plydata import *


# %% Initial Settings: Set Project Path
OS = platform.system()

if OS == "Linux":
    PROJECT_PATH = "/home/pooya/w/HydroTech/"
else:
    PROJECT_PATH = "c:/w/HydroTech/"


# %% Initial Settings: Load Required Functions

# Function 01 - Extract 'Region', 'District' from 'Peyman' Column
def extractRD(x, para):

    if para == 'ناحیه':
        if para not in x:
            return np.nan
        elif (x.index(para) + 1) >= len(x):
            return np.nan
        else:
            return str(x[x.index(para) + 1]).zfill(2)

    if para == 'منطقه':
        if para not in x:
            if ('کمربند' in x) and ('جنوبی' in x):
                return '14'
            elif ('کمربند' in x) and ('شمالی' in x):
                return '15'
            elif ('سازمان' in x) and ('پارک‌ها' in x):
                return '16'
            else:
                return np.nan
        elif (x.index(para) + 1) >= len(x):
            return np.nan
        else:
            if str(x[x.index(para) + 1]) == 'ثامن':
                return '13'
            else:
                return str(x[x.index(para) + 1]).zfill(2)


# %% Load Data: Read Data
raw_data = pd.read_excel(PROJECT_PATH + 'Data/Processed_Data/Merged_Data.xlsx')


# %% Data Cleansing: Remove Duplicated Rows
# 01. Report Duplicated Rows
tmp = raw_data[
    raw_data.duplicated(
        subset=list(raw_data.columns)[1:],
        keep=False
    )
]

tmp = tmp.sort_values(['پیمان', 'نام لکه', 'نوع قلم', 'زیرمجموعه هر قلم'])

tmp.to_excel(PROJECT_PATH + "Report/Duplicate_Rows.xlsx", index=False)

print(f"Total Number of Duplicate Rows in a Data: {tmp.shape[0]}")

tmp = raw_data[
    raw_data.duplicated(
        subset=list(raw_data.columns)[1:],
        keep='first')
]

print(f"Total Number of Duplicate Items in a Data: {tmp.shape[0]}")

del tmp

# 02. Remove Duplicated Rows
raw_data = raw_data.drop_duplicates(
    subset=list(raw_data.columns)[1:],
    keep='first'
)

print(f"Data Size: {raw_data.shape}")


# %% Data Cleansing: Remove Some Rows
tmp = raw_data["نوع آیتم"] == "حجمی"
tmp = tmp[tmp.values == True]

raw_data.drop(
    index=tmp.index,
    inplace=True
)

del tmp

print(f"Data Size: {raw_data.shape}")


# %% Data Cleansing: Extract Region And District
tmp = raw_data["پیمان"].str.strip().str.split()

raw_data["Region"] = tmp.apply(extractRD, para="منطقه")

raw_data["District"] = tmp.apply(extractRD, para="ناحیه")

del tmp


# %% Data Cleansing: Extract Peyman
# Extract Uniqe Peyman
tmp = raw_data.groupby(['Region', 'District'])['پیمان']
tmp = tmp.value_counts(dropna=False, sort=False)
tmp = pd.DataFrame(tmp)
tmp = tmp.rename(columns={'پیمان': 'Count'}).reset_index()

Peyman = []
for R in list(tmp['Region'].unique()):
    tmpR = tmp[tmp['Region'] == R]
    for D in list(tmpR['District'].unique()):
        tmpD = tmpR[tmpR['District'] == D]
        Peyman += list(range(1, len(tmpD) + 1))

tmp['Peyman'] = Peyman
tmp['Peyman'] = tmp['Peyman'].astype(str).str.zfill(2)

# Add Peyman To raw_data
tmp = tmp >> select('Region', 'District', 'پیمان', 'Peyman')

raw_data = pd.merge(raw_data,
                    tmp,
                    how='left',
                    on=['Region', 'District', 'پیمان'])

del tmp, tmpR, tmpD


# %% Data Cleansing: Extract Address
# Extract Uniqe Address
tmp = raw_data.groupby(['Region', 'District', 'Peyman'])['نام لکه']
tmp = tmp.value_counts(dropna=False, sort=False)
tmp = pd.DataFrame(tmp)
tmp = tmp.rename(columns={'نام لکه': 'Count'}).reset_index()

Address = []
for R in list(tmp['Region'].unique()):
    tmpR = tmp[tmp['Region'] == R]
    for D in list(tmpR['District'].unique()):
        tmpD = tmpR[tmpR['District'] == D]
        for P in list(tmpD['Peyman'].unique()):
            tmpP = tmpD[tmpD['Peyman'] == P]
            Address += list(range(1, len(tmpP) + 1))

tmp['Address'] = Address
tmp['Address'] = tmp['Address'].astype(str).str.zfill(3)

# Add Address To raw_data
tmp = tmp >> select('Region', 'District', 'Peyman', 'نام لکه', 'Address')

raw_data = pd.merge(raw_data,
                    tmp,
                    how='left',
                    on=['Region', 'District', 'Peyman', 'نام لکه'])

del tmp, tmpR, tmpD, tmpP


# %% Report: Check Region
raw_data['Region'].value_counts(dropna=False, sort=True)


# %% Report: Check District
raw_data['District'].value_counts(dropna=False, sort=True)


# %% Report: Check Peyman
raw_data['Peyman'].value_counts(dropna=False)


# %% Report: Check Address
raw_data['Address'].value_counts(dropna=False)


# %% Report: Check Region, District and Peyman
tmp = raw_data.astype(str).groupby(['Region', 'District', 'Peyman', 'پیمان'])
tmp = pd.DataFrame(tmp.size())
tmp = tmp.reset_index()
tmp = tmp.rename(columns={'Region': 'منطقه',
                          'District': 'ناحیه',
                          'Peyman': 'پیمان',
                          'پیمان': 'نام پیمان',
                          0: 'تعداد'})

file_name = PROJECT_PATH + "Report/Region_District_Peyman.xlsx"
tmp.to_excel(file_name, index=False)

print(tmp)
del tmp, file_name

# %% Report: Check Region, District, Peyman, Address
tmp = raw_data.astype(str).groupby(
    ['Region', 'District', 'Peyman', 'Address', 'نام لکه'])
tmp = pd.DataFrame(tmp.size())
tmp = tmp.reset_index()
tmp = tmp.rename(columns={'Region': 'منطقه',
                          'District': 'ناحیه',
                          'Peyman': 'پیمان',
                          'Address': 'لکه',
                          0: 'تعداد'})

file_name = PROJECT_PATH + "Report/Region_District_Peyman_Address.xlsx"
tmp.to_excel(file_name, index=False)

print(tmp)
del tmp, file_name


# %%
