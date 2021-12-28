

import pandas as pd
import sqlalchemy
import pyodbc
from sqlalchemy import create_engine


#setup SQL Alchemy Engine, assumes user has setup or has access a microsoft SQL Server, username, password, database and permissions to create and alter tables in the database.
driver = 'ODBC Driver 17 for SQL Server'
servername = 'Enter SQL SERVER' #sql server name/ip address
database = 'Enter Database' #sql server database power_outage_data
username = 'Enter Username'  #sql server username
password = 'Enter Password' #sql server password
engine = sqlalchemy.create_engine('mssql+pyodbc://'+username+":"+password+"@"+servername+'/'+database+'?driver=' + driver, fast_executemany=True)


#read the compressed raw data files from github into pandas dataframes
hist_weather = pd.read_csv('https://github.com/VenkataVanga/MSDS-6120_Capstone/blob/main/Data/historical_weather.csv.gz?raw=tr', compression='gzip')
hist_da_price = pd.read_csv('https://github.com/VenkataVanga/MSDS-6120_Capstone/blob/main/Data/historical_dayahead_price.csv.gz?raw=tr', compression='gzip')
hist_load = pd.read_csv('https://github.com/VenkataVanga/MSDS-6120_Capstone/blob/main/Data/historical_load.csv.gz?raw=tr', compression='gzip')
hist_rt_price = pd.read_csv('https://github.com/VenkataVanga/MSDS-6120_Capstone/blob/main/Data/historical_realtime_price.csv.gz?raw=tr', compression='gzip')
hist_outage_data = pd.read_csv('https://github.com/VenkataVanga/MSDS-6120_Capstone/blob/main/Data/historical_outage.csv.gz?raw=tr', compression='gzip')
date_mapping = pd.read_csv('https://github.com/VenkataVanga/MSDS-6120_Capstone/blob/main/Data/date_mapping.csv.gz?raw=tr', compression='gzip')
timestamp_mapping = pd.read_csv('https://github.com/VenkataVanga/MSDS-6120_Capstone/blob/main/Data/timestamp_mapping.csv.gz?raw=tr', compression='gzip')


#write dataframes to SQL Database
hist_weather.to_sql("historic_weather_data", engine, schema='dbo', index=False, if_exists='replace')
hist_da_price.to_sql("ercot_historic_da_price_data", engine, schema='dbo', index=False, if_exists='replace')
hist_load.to_sql("ercot_historic_load_data", engine, schema='dbo', index=False, if_exists='replace')
hist_rt_price.to_sql("ercot_historic_rt15_price_data", engine, schema='dbo', index=False, if_exists='replace')
date_mapping.to_sql("dimdate_dst_table", engine, schema='dbo', index=False, if_exists='replace')
timestamp_mapping.to_sql("DST_timestamp_mapping_table", engine, schema='dbo', index=False, if_exists='replace')
hist_outage_data.to_sql("poweroutage_historic_data", engine, schema='dbo', index=False, if_exists='replace')


#######################################################execute SQL data transformations in the SQL Server


#aggregate the outage data on a county level
from sqlalchemy.sql import text
with engine.connect().execution_options(autocommit=True) as con:
        
    county_agg_statement = text(r'''
IF OBJECT_ID('dbo.poweroutage_historic_data_countylevel_aggregation') IS NOT NULL DROP TABLE poweroutage_historic_data_countylevel_aggregation;
 select distinct
[State]	
,[County]	
,RecordDateTime_EST	
,sum(Customer_Count) as Customer_Count	
,sum(Outage_Count) as Outage_Count
into [dbo].[poweroutage_historic_data_countylevel_aggregation]
from [dbo].[poweroutage_historic_data]
group by
[State]	
,[County]	
,[RecordDateTime_EST]''')

    con.execute(county_agg_statement)



#pivot the weather features data so it can be merged with outage data
with engine.connect().execution_options(autocommit=True) as con:
    weather_data_pivot_statement = text(r'''

-------------------Create pivot/one hot encoding of concatnation of weather main and weather description, there are multiple descriptions for the same hour ending.
if object_id('tempdb..#weatherpivot') is not null drop table #weatherpivot
select * 
into #weatherpivot
from (
select distinct
[county]
,RecordDateTime_UTC
,weather_main_with_description
from (
select 
[county] = (case
when city_name ='Brazoria County' then 'Brazoria'
when city_name ='Collin County' then 'Collin'
when city_name ='Dallas County' then 'Dallas'
when city_name ='Denton County' then 'Denton'
when city_name ='Fort Bend County' then 'Fort Bend'
when city_name ='Galveston County' then 'Galveston'
when city_name ='Harris County' then 'Harris'
when city_name ='Montgomery County' then 'Montgomery'
when city_name ='Tarrant County' then 'Tarrant' end)
,cast(left(dt_iso,len(dt_iso)-10) as datetime) as RecordDateTime_UTC
,cast( (cast(left(dt_iso,len(dt_iso)-10) as datetime) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time') as datetime) as RecordDateTime_CST
,weather_main_with_description = concat(weather_main,'_',weather_description)
,*		 
from [dbo].[historic_weather_data] 
) t 
) a
pivot
(
  count([weather_main_with_description])
  for [weather_main_with_description] in ([Clear_sky is clear]
,[Clouds_broken clouds]
,[Clouds_few clouds]
,[Clouds_overcast clouds]
,[Clouds_scattered clouds]
,[Drizzle_drizzle]
,[Drizzle_heavy intensity drizzle]
,[Drizzle_light intensity drizzle]
,[Dust_dust]
,[Fog_fog]
,[Haze_haze]
,[Mist_mist]
,[Rain_extreme rain]
,[Rain_freezing rain]
,[Rain_heavy intensity rain]
,[Rain_heavy intesity shower rain]
,[Rain_light rain]
,[Rain_moderate rain]
,[Rain_proximity shower rain]
,[Rain_shower rain]
,[Rain_very heavy rain]
,[Smoke_smoke]
,[Snow_heavy snow]
,[Snow_light rain and snow]
,[Snow_light snow]
,[Snow_snow]
,[Squall_squalls]
,[Thunderstorm_proximity thunderstorm]
,[Thunderstorm_proximity thunderstorm with rain]
,[Thunderstorm_ragged thunderstorm]
,[Thunderstorm_thunderstorm]
,[Thunderstorm_thunderstorm with heavy rain]
,[Thunderstorm_thunderstorm with light rain]
,[Thunderstorm_thunderstorm with rain]
)
) piv;



IF OBJECT_ID('dbo.historic_weather_data_PIVOTED') IS NOT NULL DROP TABLE historic_weather_data_PIVOTED;
select distinct
t.county
,t.RecordDateTime_UTC
,t.temp	
,t.feels_like	
,t.temp_min	
,t.temp_max	
,t.pressure	
,t.sea_level	
,t.grnd_level	
,t.humidity	
,t.wind_speed	
,t.wind_deg	
,t.rain_1h	
,t.rain_3h	
,t.snow_1h	
,t.snow_3h	
,t.clouds_all
,[Clear_sky is clear]
,[Clouds_broken clouds]
,[Clouds_few clouds]
,[Clouds_overcast clouds]
,[Clouds_scattered clouds]
,[Drizzle_drizzle]
,[Drizzle_heavy intensity drizzle]
,[Drizzle_light intensity drizzle]
,[Dust_dust]
,[Fog_fog]
,[Haze_haze]
,[Mist_mist]
,[Rain_extreme rain]
,[Rain_freezing rain]
,[Rain_heavy intensity rain]
,[Rain_heavy intesity shower rain]
,[Rain_light rain]
,[Rain_moderate rain]
,[Rain_proximity shower rain]
,[Rain_shower rain]
,[Rain_very heavy rain]
,[Smoke_smoke]
,[Snow_heavy snow]
,[Snow_light rain and snow]
,[Snow_light snow]
,[Snow_snow]
,[Squall_squalls]
,[Thunderstorm_proximity thunderstorm]
,[Thunderstorm_proximity thunderstorm with rain]
,[Thunderstorm_ragged thunderstorm]
,[Thunderstorm_thunderstorm]
,[Thunderstorm_thunderstorm with heavy rain]
,[Thunderstorm_thunderstorm with light rain]
,[Thunderstorm_thunderstorm with rain]
into [dbo].[historic_weather_data_PIVOTED]
from (
select 
[county] = (case
when city_name ='Brazoria County' then 'Brazoria'
when city_name ='Collin County' then 'Collin'
when city_name ='Dallas County' then 'Dallas'
when city_name ='Denton County' then 'Denton'
when city_name ='Fort Bend County' then 'Fort Bend'
when city_name ='Galveston County' then 'Galveston'
when city_name ='Harris County' then 'Harris'
when city_name ='Montgomery County' then 'Montgomery'
when city_name ='Tarrant County' then 'Tarrant' end)
,cast(left(dt_iso,len(dt_iso)-10) as datetime) as RecordDateTime_UTC
,cast( (cast(left(dt_iso,len(dt_iso)-10) as datetime) AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time') as datetime) as RecordDateTime_CST
,weather_main_with_description = concat(weather_main,'_',weather_description)
,*		 
from [dbo].[historic_weather_data] 
) t 
left join #weatherpivot wpvt
on t.county = wpvt.county
and t.RecordDateTime_UTC = wpvt.RecordDateTime_UTC
''')

    con.execute(weather_data_pivot_statement)



#merge outage and weather data
with engine.connect().execution_options(autocommit=True) as con:
        
    pivot_with_outage_statement = text(r'''
if object_id('tempdb..#data_comb1') is not null drop table #data_comb1
select p.* 	
,wth.temp	
,wth.feels_like	
,wth.temp_min	
,wth.temp_max	
,wth.pressure	
,wth.sea_level	
,wth.grnd_level	
,wth.humidity	
,wth.wind_speed	
,wth.wind_deg	
,wth.rain_1h	
,wth.rain_3h	
,wth.snow_1h	
,wth.snow_3h	
,wth.clouds_all
,wth.[Clear_sky is clear]
,wth.[Clouds_broken clouds]
,wth.[Clouds_few clouds]
,wth.[Clouds_overcast clouds]
,wth.[Clouds_scattered clouds]
,wth.[Drizzle_drizzle]
,wth.[Drizzle_heavy intensity drizzle]
,wth.[Drizzle_light intensity drizzle]
,wth.[Dust_dust]
,wth.[Fog_fog]
,wth.[Haze_haze]
,wth.[Mist_mist]
,wth.[Rain_extreme rain]
,wth.[Rain_freezing rain]
,wth.[Rain_heavy intensity rain]
,wth.[Rain_heavy intesity shower rain]
,wth.[Rain_light rain]
,wth.[Rain_moderate rain]
,wth.[Rain_proximity shower rain]
,wth.[Rain_shower rain]
,wth.[Rain_very heavy rain]
,wth.[Smoke_smoke]
,wth.[Snow_heavy snow]
,wth.[Snow_light rain and snow]
,wth.[Snow_light snow]
,wth.[Snow_snow]
,wth.[Squall_squalls]
,wth.[Thunderstorm_proximity thunderstorm]
,wth.[Thunderstorm_proximity thunderstorm with rain]
,wth.[Thunderstorm_ragged thunderstorm]
,wth.[Thunderstorm_thunderstorm]
,wth.[Thunderstorm_thunderstorm with heavy rain]
,wth.[Thunderstorm_thunderstorm with light rain]
,wth.[Thunderstorm_thunderstorm with rain]
into #data_comb1
from (
select 
[Metro_Area] = (case
when county in ('Harris','Fort Bend','Brazoria','Galveston','Montgomery') then 'Houston'
else 'Dallas' End)
,cast( (cast(RecordDateTime_EST as datetime) AT TIME ZONE 'Eastern Standard Time' AT TIME ZONE 'Central Standard Time') as datetime) as RecordDateTime_CST
,cast( (cast(RecordDateTime_EST as datetime) AT TIME ZONE 'Eastern Standard Time' AT TIME ZONE 'UTC') as datetime) as RecordDateTime_UTC
,*
from [dbo].[poweroutage_historic_data_countylevel_aggregation] ) p

left join [power_outage_data].[dbo].[historic_weather_data_PIVOTED] wth
on p.county = wth.county
and cast(p.RecordDateTime_UTC as datetime) = cast(wth.RecordDateTime_UTC as datetime)


IF OBJECT_ID('dbo.historic_weather_data_PIVOTED_with_OUTAGE_DATA') IS NOT NULL DROP TABLE historic_weather_data_PIVOTED_with_OUTAGE_DATA;
select * 
into [dbo].[historic_weather_data_PIVOTED_with_OUTAGE_DATA]
from #data_comb1
''')

    con.execute(pivot_with_outage_statement)



#merge weather, outage and ercot data
with engine.connect().execution_options(autocommit=True) as con:
        
    pivot_with_all_statement = text(r'''

------------- outage all features

-----------------historical load
if object_id('tempdb..#historicals') is not null drop table #historicals
select 
cast( (cast(Datetime_CST as datetime) AT TIME ZONE 'Central Standard Time' AT TIME ZONE 'UTC') as datetime) as RecordDateTime_UTC
,ii.* 
into #historicals
from (
select 
cast(concat([Date],' ',concat((case when len([time])=4 then '0' else '' end),[time])) as datetime) as Datetime_CST,
--concat((case when len([time])=4 then '0' else '' end),[time]) as timecheck,
t.* from (
select 
--cast( (cast([Hour Ending] as datetime) AT TIME ZONE 'Central Standard Time' AT TIME ZONE 'UTC') as datetime) as RecordDateTime_UTC
[Date] = (case when right([Hour Ending],5) = '24:00' then dateadd(day,1,cast(left([Hour Ending],10) as date)) else cast(left([Hour Ending],10) as date) end)
,[time] = rtrim(ltrim((case when right([Hour Ending],5) = '24:00' then '00:00' else right([Hour Ending],5) end)))
,* from [dbo].[ercot_historic_load_data] 
where [Hour Ending] not like '%DST%'
) t ) ii




---historical DA prices
if object_id('tempdb..#daprice') is not null drop table #daprice
select * 
into #daprice
from (
select ddd.RecordDateTime_UTC
,[Settlement Point]	
,[Price]
from(
select 
cast( (cast(Datetime_CST as datetime) AT TIME ZONE 'Central Standard Time' AT TIME ZONE 'UTC') as datetime) as RecordDateTime_UTC
,cast(rtrim(ltrim(REPLACE([Settlement Point Price],',',''))) as decimal(20,5)) as [Price]
,dd.* from (
select 
cast(concat([Date],' ',concat((case when len([time])=4 then '0' else '' end),[time])) as datetime) as Datetime_CST,
d.* from (
select 
[Date] = (case when [Hour Ending] = '24:00' then dateadd(day,1,cast([Delivery Date] as date)) else cast([Delivery Date] as date) end)
,[time] = rtrim(ltrim((case when [Hour Ending] = '24:00' then '00:00' else [Hour Ending] end)))
,* from [dbo].[ercot_historic_da_price_data] 
where [Repeated Hour Flag] ='N'
) d ) dd ) ddd ) a
pivot
(
  sum([Price])
  for [Settlement Point] in ([LZ_HOUSTON]
,[LZ_NORTH]
,[HB_HOUSTON]
,[HB_NORTH]
)
) piv;




-----historical RT prices

if object_id('tempdb..#rtprice') is not null drop table #rtprice
select * 
into #rtprice
from (
select
RecordDateTime_UTC
,[Settlement Point Name]
,[Settlement Point Price]
from (
select 
cast( (cast(Datetime_CST as datetime) AT TIME ZONE 'Central Standard Time' AT TIME ZONE 'UTC') as datetime) as RecordDateTime_UTC
--,cast(rtrim(ltrim(REPLACE(REPLACE([Settlement Point Price],',',''),'-',''))) as decimal(20,5)) as [Price]
--,cast(rtrim(ltrim(REPLACE([Settlement Point Price],',',''))) as decimal(20,5)) as [Price]
,*
from (
select 
cast(concat([Date],' ',concat(concat((case when len([Hour Ending])=1 then '0' else '' end),[Hour Ending]),' : 00 ')) as datetime) as Datetime_CST
,* 
from (
select
[Date] = (case when [Delivery Hour] = 24 then dateadd(day,1,cast([Delivery Date] as date)) else cast([Delivery Date] as date) end)
,[Hour Ending] = (case when [Delivery Hour] = 24 then 0 else [Delivery Hour] end)
,* from (
select distinct
[Delivery Date]	
,[Delivery Hour]
,[Settlement Point Name]
,avg([Settlement Point Price]) as [Settlement Point Price]
from [dbo].[ercot_historic_rt15_price_data]
where [Repeated Hour Flag] ='N'
group by [Delivery Date]	
,[Delivery Hour]
,[Settlement Point Name] ) r ) r2
) r3 ) x ) a



pivot
(
  sum([Settlement Point Price])
  for [Settlement Point Name] in ([LZ_HOUSTON]
,[LZ_NORTH]
,[HB_HOUSTON]
,[HB_NORTH]
)
) piv;

IF OBJECT_ID('dbo.historic_weather_data_PIVOTED_with_ALL_FEATURES') IS NOT NULL DROP TABLE historic_weather_data_PIVOTED_with_ALL_FEATURES;
select  o.* 
,ERCOT_WEATHERZONE_LOAD = (Case when o.Metro_Area = 'Houston' then l.COAST else l.NCENT end)
,ERCOT_RT_LOADZONE_PRICE = (Case when o.Metro_Area = 'Houston' then rt.LZ_HOUSTON else rt.LZ_NORTH end)
,ERCOT_RT_HUB_PRICE = (Case when o.Metro_Area = 'Houston' then rt.HB_HOUSTON else rt.HB_NORTH end)
,ERCOT_DA_LOADZONE_PRICE = (Case when o.Metro_Area = 'Houston' then da.LZ_HOUSTON else da.LZ_NORTH end)
,ERCOT_DA_HUB_PRICE = (Case when o.Metro_Area = 'Houston' then da.HB_HOUSTON else da.HB_NORTH end)

into [dbo].[historic_weather_data_PIVOTED_with_ALL_FEATURES]
from [dbo].[historic_weather_data_PIVOTED_with_OUTAGE_DATA] o

left join #historicals l
on o.RecordDateTime_UTC = l.RecordDateTime_UTC

left join  #rtprice rt
on o.RecordDateTime_UTC = rt.RecordDateTime_UTC

left join  #daprice da
on o.RecordDateTime_UTC = da.RecordDateTime_UTC

''')

    con.execute(pivot_with_all_statement)


