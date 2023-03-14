#%%
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import pandas as pd
import os

os.chdir("C:\\Users\\SQLe\\Data\\")
year="2021"
#%%
#get column names
colnames=pd.read_table("C:\\Users\\SQLe\\Data\\FY21_02_22_2022_Standard_Credit_READ_ME.txt",delimiter=" ",skiprows=21
                       ,skipfooter=2,header=None,engine="python")
colnames=colnames.iloc[:,[3,4]]
colnames.columns=("header","type")
colnames['header']=colnames['header'].str.upper()
colnames.set_index("header",inplace=True)
#%%
schema=pq.ParquetDataset("./SBGR_final/",validate_schema=True).schema

#%%
def skip(row):
        return 'skip'

read_options=csv.ReadOptions(
   encoding="latin1",
   column_names=colnames.index
 #  ,skip_rows=2475141
   )
parse_options=csv.ParseOptions(delimiter="^",quote_char=False
                               ,newlines_in_values=True
                               ,invalid_row_handler=skip)
convert_options=csv.ConvertOptions(
   check_utf8=False,
   column_types=schema)

PT=csv.read_csv("C:\\Users\\SQLe\\Data\\FY21 data\\FY21_02222022_SC_FINAL.csv"
                ,read_options=read_options,parse_options=parse_options,convert_options=convert_options)

# %%
PT.schema
PT.num_rows



#%%
def drop_na_columns(df):
    null_columns = []
    schema = df.schema
    for (name_, type_) in zip(schema.names, schema.types):
        if type_ == pa.null():
          null_columns.append(name_)
    return df.drop(null_columns)

PT=drop_na_columns(PT)

# %%
os.mkdir(f"./SBGR_final/FY={year}")
pq.write_table(PT,f'./SBGR_final/FY={year}/part-0.parquet')

# %%
