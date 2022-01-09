import pandas as pd
import sqlalchemy
import pyodbc
from sqlalchemy import create_engine
import tkinter as tk
from tkinter import *
from tkinter import filedialog



root = Tk()

csvpath = filedialog.askdirectory(title = "Select where csv.gz will be stored")
csvpath = csvpath.replace('/', '\\')

csvpath_final = csvpath+'\\'+'timeseries_outage_raw_data.csv.gz'

root.destroy()


#setup SQL Alchemy Engine, assumes user has setup or has access a microsoft SQL Server, username, password, database and permissions to create and alter tables in the database.
driver = 'ODBC Driver 17 for SQL Server'
servername = 'Enter SQL Server IP ADDRESS' #sql server name/ip address
database = 'Enter Database' #sql server database power_outage_data
username = 'Enter Username'  #sql server username
password = 'Enter Password' #sql server password
engine = sqlalchemy.create_engine('mssql+pyodbc://'+username+":"+password+"@"+servername+'/'+database+'?driver=' + driver, fast_executemany=True)



outage_query = f'''

select 
 cast((case when County in ('Harris','Tarrant','Dallas', 'Montgomery', 'Brazoria') then 'Train' else 'Validate' end) as varchar(50)) as [set_type] 
,cast(County as varchar(50)) as County
,cast(RecordDateTime_UTC as datetime) as RecordDateTime_UTC
,outage_percent
,cast(temp as float) as  temp
,cast(feels_like as float)  as feels_like
,cast(temp_min as float)  as temp_min	
,cast(temp_max as float)  as temp_max
,cast(pressure as float)  as pressure
,cast(humidity as float) 	as humidity
,cast(wind_speed as float) 	as wind_speed
,cast(wind_deg as float) as wind_deg
,cast(isnull(rain_1h,0) as float)  as rain_1h
,cast(  REPLACE(ERCOT_WEATHERZONE_LOAD, ',', '') as float) as ERCOT_WEATHERZONE_LOAD	
,cast(ERCOT_RT_LOADZONE_PRICE as float) as ERCOT_RT_LOADZONE_PRICE	
,cast(ERCOT_RT_HUB_PRICE as float) 	as ERCOT_RT_HUB_PRICE
,cast(ERCOT_DA_LOADZONE_PRICE as float) as ERCOT_DA_LOADZONE_PRICE	
,cast(ERCOT_DA_HUB_PRICE as float) as ERCOT_DA_HUB_PRICE

  from (
  select 
  outage_percent = (case when Customer_Count = 0 then 0
  when Outage_Count > Customer_Count then 1
  else cast(Outage_Count as float)/cast(Customer_Count as float)  end)

  ,* from [power_outage_data].[dbo].[historic_weather_data_PIVOTED_with_ALL_FEATURES]
  ) x
  where  [ERCOT_WEATHERZONE_LOAD] is not null
  and ERCOT_WEATHERZONE_LOAD <> 'NULL'
and [ERCOT_RT_LOADZONE_PRICE]  is not null
and [ERCOT_RT_HUB_PRICE]  is not null
and [ERCOT_DA_LOADZONE_PRICE]  is not null
and [ERCOT_DA_HUB_PRICE]  is not null
  order by County, cast(RecordDateTime_UTC as datetime)

'''

outage_df = pd.read_sql_query(outage_query, engine)


outage_df.to_csv(csvpath_final, 
           index=False, 
           compression="gzip")