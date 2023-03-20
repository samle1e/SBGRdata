#%%
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as csv
import pyarrow.compute as pc
import pandas as pd
import os

os.chdir("C:\\Users\\SQLe\\Data\\SBGR_final")
#%%
#create unified schema
schema22=pq.ParquetDataset("2022.parquet").schema
schema21=pq.ParquetDataset("2021.parquet").schema
unified_schema=pa.unify_schemas([schema22,schema21])
#%%
types=unified_schema.types
decimals = [i for i, x in enumerate(types) if pa.types.is_decimal(x)]

for i in decimals:    
    unified_schema=unified_schema.set(i,unified_schema.field(i).with_type(pa.float64()))
#replace decimals with float64
empty_table=unified_schema.empty_table()

#%%
for year in reversed(range(2009,2023)):
    print(f"Starting {year}")
    table=pq.read_table(f"{year}.parquet",schema=unified_schema)
    if ((year==2009) | (year==2022)):
        table=pa.concat_tables([empty_table,table],promote=True)
    
    os.mkdir(f"./Parquet/FY={year}")
    pq.write_table(table,f'./Parquet/FY={year}/part-0.parquet')

    unified_schema=pa.unify_schemas([unified_schema,table.schema])
    print(f"Done with {year}")
# %%
#test the dataset
test=pq.ParquetDataset("./Parquet",validate_schema=True)
columns=test.read(columns=['FY','TOTAL_SB_ACT_ELIGIBLE_DOLLARS', 'SMALL_BUSINESS_DOLLARS'])
# %%
