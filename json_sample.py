# https://towardsdatascience.com/all-pandas-json-normalize-you-should-know-for-flattening-json-13eae1dfb7dd
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.json_normalize.html

import pandas as pd

data = {"something1":[{"@context":"ABC","entity":"PQR","URL":"abc@yahoo.com"}],
        "something2":[{"@context":"RST","entity":"UVW","URL":"efg@yahoo.com"},{"@context":"bfsh","entity":"UVW","URL":"fnksdfj@yahoo.com"}]
        }

startRow = 0

writer = pd.ExcelWriter("C:\\Users\\Vicky\\Minnie\\json_to_excel.xlsx", engine='xlsxwriter')
workbook = writer.book
worksheet = workbook.add_worksheet("Result")
writer.sheets["Result"] = worksheet

for key in data.keys():
    print(key)
    worksheet.write(startRow, 0, key)
    startRow += 1

    df = pd.json_normalize(data, record_path=[key])
    print(df.to_string(index=False))
    print()
    df.to_excel(writer, sheet_name='Result', startrow=startRow, index=False)
    startRow += len(df) + 2

writer.save()
print("Data written to excel file successfully.")
