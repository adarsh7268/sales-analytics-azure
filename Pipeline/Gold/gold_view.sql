--create view CALENDER

Create view gold.calender as


Select * from OPENROWSET(
    BULK 'https://adstorage1234.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT = 'PARQUET'
) as query1



--create view Customers

Create view gold.customer as


Select * from OPENROWSET(
    BULK 'https://adstorage1234.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT = 'PARQUET'
) as query1





--create view Products

Create view gold.product as


Select * from OPENROWSET(
    BULK 'https://adstorage1234.dfs.core.windows.net/silver/AdventureWorks_Products/',
    FORMAT = 'PARQUET'
) as query1




--create view Returns

Create view gold.returns as


Select * from OPENROWSET(
    BULK 'https://adstorage1234.dfs.core.windows.net/silver/AdventureWorks_Returns/',
    FORMAT = 'PARQUET'
) as query1




--create view Sales
Create view gold.sales as


Select * from OPENROWSET(
    BULK 'https://adstorage1234.dfs.core.windows.net/silver/AdventureWorks_Sales/',
    FORMAT = 'PARQUET'
) as query1



--create view Subcategories

Create view gold.subcategories as


Select * from OPENROWSET(
    BULK 'https://adstorage1234.dfs.core.windows.net/silver/AdventureWorks_Subcategories/',
    FORMAT = 'PARQUET'
) as query1





--create view Territories

Create view gold.territories as


Select * from OPENROWSET(
    BULK 'https://adstorage1234.dfs.core.windows.net/silver/AdventureWorks_Territories/',
    FORMAT = 'PARQUET'
) as query1

