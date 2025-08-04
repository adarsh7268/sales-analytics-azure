---CREATE MASTER KEY ENCRYPTION BY PASSWORD ='root@123'

-- CREATE DATABASE SCOPED CREDENTIAL cred_ad
-- WITH
--     IDENTITY = 'Managed Identity'

-- CREATE EXTERNAL DATA SOURCE source_silver
-- WITH
-- (
--     LOCATION='https://adstorage1234.dfs.core.windows.net/silver',
--     CREDENTIAL= cred_ad
-- )


-- CREATE EXTERNAL DATA SOURCE source_gold
-- WITH
-- (
--     LOCATION='https://adstorage1234.dfs.core.windows.net/gold',
--     CREDENTIAL= cred_ad
-- )

-- CREATE EXTERNAL FILE FORMAT format_parquet
-- WITH
-- (
--     FORMAT_TYPE=PARQUET,
--     DATA_COMPRESSION='org.apache.hadoop.io.compress.SnappyCodec'
-- )




Create EXTERNAL TABLE gold.extsales
WITH
(
    LOCATION='extslales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT=format_parquet
)

AS 
Select * from gold.sales

select * from gold.extsales