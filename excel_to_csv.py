import pandas as pd

dataframe = pd.read_excel("C:\\Users\\Vicky\\Minnie\\openpyxl.xlsx", sheet_name=None)
first = True

for key in dataframe:
    if first:
        dataframe[key].to_csv("C:\\Users\\Vicky\\Minnie\\excel_to_csv.csv", index=False, mode='w')
        first = False
    else:
        dataframe[key].to_csv("C:\\Users\\Vicky\\Minnie\\excel_to_csv.csv", index=False, mode='a')
