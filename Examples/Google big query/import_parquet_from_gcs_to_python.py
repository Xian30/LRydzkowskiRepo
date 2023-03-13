#import parquet from gcs

import dask.dataframe as dd
import pandas as pd    
path = "gcs://bucket/folder/
ddf = dd.read_parquet(path, engine="pyarrow",ignore_metadata_file=True ,index=False,storage_options={'token': 'cloud'})
df = ddf.compute()` 

