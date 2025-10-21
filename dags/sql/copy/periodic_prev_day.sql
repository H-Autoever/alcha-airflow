copy staging.periodic_data
from 's3://team-alcha-etl-stage/curated/periodic/year={{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%Y") }}/month={{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%m") }}/day={{ macros.ds_format(macros.ds_add(ds, -1), "%Y-%m-%d", "%d") }}/'
iam_role default
format as parquet;

