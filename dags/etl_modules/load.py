from dags.etl_modules.transform import transform
import pandas as pd


paths_parquet = transform()

print(pd.read_parquet(paths_parquet['weather']))
