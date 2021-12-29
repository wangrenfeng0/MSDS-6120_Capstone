

import pandas as pd 
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
import pytz
from pytz import utc
from pytz import timezone
import requests
from lxml import html
from pandasql import sqldf

#get todays and yesterdays date

today1 = datetime.today()
yesterday1 = today1 - timedelta(days = 1)
today1_str = today1.strftime('%Y%m%d')
yesterday1_str = yesterday1.strftime('%Y%m%d')

#today1_str
#yesterday1_str
#today1_str = '20211230'




###########################################################live and recent ERCOT load

try:
    load1 = pd.read_html(r'''https://www.ercot.com/content/cdr/html/'''+str(today1_str)+'''_actual_loads_of_weather_zones.html''')[0]
    load1 = load1.rename(columns=load1.iloc[0]).loc[1:]
    load1['MWH_load'] = 'MWH_load'
    load2 = pd.read_html(r'''https://www.ercot.com/content/cdr/html/'''+str(yesterday1_str)+'''_actual_loads_of_weather_zones.html''')[0]
    load2 = load2.rename(columns=load2.iloc[0]).loc[1:]
    load2['MWH_load'] = 'MWH_load'
    load3 = pd.concat([load1,load2]).reset_index()
    load3['Oper Day'] = pd.to_datetime(load3['Oper Day'], format='%m/%d/%Y')
    load3['rank'] = load3[["Oper Day","Hour Ending"]].apply(tuple,axis=1).rank(method='dense',ascending=False).astype(int)
    load3 = load3.sort_values("rank")


except:
    load2 = pd.read_html(r'''https://www.ercot.com/content/cdr/html/'''+str(yesterday1_str)+'''_actual_loads_of_weather_zones.html''')[0]
    load2 = load2.rename(columns=load2.iloc[0]).loc[1:]
    load2['MWH_load'] = 'MWH_load'
    load3 = load2.reset_index()
    load3['Oper Day'] = pd.to_datetime(load3['Oper Day'], format='%m/%d/%Y')
    load3['rank'] = load3[["Oper Day","Hour Ending"]].apply(tuple,axis=1).rank(method='dense',ascending=False).astype(int)
    load3 = load3.sort_values("rank")


load4 = load3.reset_index()[['rank','Oper Day', 'Hour Ending','COAST','NORTH_C']].copy()
load4
#Coast is Houston counties, North_C is Dallas Counties




###########################################################live and recent ERCOT RT SPP


try:
    rt1 = pd.read_html(r'''https://www.ercot.com/content/cdr/html/'''+str(today1_str)+'''_real_time_spp.html''')[0]
    rt2 = pd.read_html(r'''https://www.ercot.com/content/cdr/html/'''+str(yesterday1_str)+'''_real_time_spp.html''')[0]
    rt3 = pd.concat([rt1,rt2]).reset_index()
except:
    rt3 = pd.read_html(r'''https://www.ercot.com/content/cdr/html/'''+str(yesterday1_str)+'''_real_time_spp.html''')[0]


from pandasql import sqldf
pysqldf = lambda q: sqldf(q, globals())

q = """

select g.* 
,(case 
when [interval_r2] = '00' then 0
when [interval_r2] = '15' then 85
when [interval_r2] = '30' then 70
when [interval_r2] = '45' then 55
end)+[Interval Ending] as Interval_round
from (
select *
,SUBSTR([Interval Ending] ,-2) as [interval_r2]
from rt3 ) g 

"""
global rt3_df
rt3_df = pysqldf(q)

rt3_df['Oper Day'] = pd.to_datetime(rt3_df['Oper Day'], format='%m/%d/%Y')

rt3_df = rt3_df[['Oper Day','Interval_round','HB_HOUSTON','HB_NORTH','LZ_HOUSTON','LZ_NORTH']].copy()

rt4_df = rt3_df.groupby(['Oper Day','Interval_round'], as_index=False).mean()

rt4_df['rank'] = rt4_df[["Oper Day","Interval_round"]].apply(tuple,axis=1).rank(method='dense',ascending=False).astype(int)
rt4_df = rt4_df.sort_values("rank")


rt4_df
#HB&LZ Houston is Houston counties, HB & LZ North is Dallas Counties



###########################################################live and recent ERCOT DA SPP

try:
    da1 = pd.read_html(r'''https://www.ercot.com/content/cdr/html/'''+str(today1_str)+'''_dam_spp.html''')[0]
    da2 = pd.read_html(r'''https://www.ercot.com/content/cdr/html/'''+str(yesterday1_str)+'''_dam_spp.html''')[0]
    da3 = pd.concat([da1,da2]).reset_index()
except:
    da3 = pd.read_html(r'''https://www.ercot.com/content/cdr/html/'''+str(yesterday1_str)+'''_dam_spp.html''')[0]

da3['Oper Day'] = pd.to_datetime(da3['Oper Day'], format='%m/%d/%Y')
da3 = da3[['Oper Day','Hour Ending','HB_HOUSTON','HB_NORTH','LZ_HOUSTON','LZ_NORTH']].copy()

da3['rank'] = da3[["Oper Day","Hour Ending"]].apply(tuple,axis=1).rank(method='dense',ascending=False).astype(int)
da3 = da3.sort_values("rank")

da3
#HB&LZ Houston is Houston counties, HB & LZ North is Dallas Counties



################################################################Power Outage US Current Snapshot

from lxml import html
import requests


def power_outage_scrape(url):
    web = requests.get(url)
    tree = html.fromstring(web.content)
    county = tree.xpath('/html/body/div[2]/div[2]/div/div[1]/h1/text()')
    customers = tree.xpath('/html/body/div[2]/div[3]/div[1]/div/div[2]/text()')
    outagecount = tree.xpath('/html/body/div[2]/div[3]/div[2]/div/div[2]/text()') 
    outagepercent = tree.xpath('/html/body/div[2]/div[3]/div[3]/div/div[2]/text()') 
    lastupdated = tree.xpath('/html/body/div[2]/div[3]/div[4]/div/div[2]/item/text()') 
    
    data = {'county': county,
        'number_of_customers': customers,
        'number_of_outages': outagecount,
        'outage_percent': outagepercent,
        'lastupdated': lastupdated,
        }

    results = pd.DataFrame(data) 
    return results

list_of_counties_website = [
'https://poweroutage.us/area/county/1394'
,'https://poweroutage.us/area/county/1386'
,'https://poweroutage.us/area/county/1381'   
 ,'https://poweroutage.us/area/county/1443'   
 ,'https://poweroutage.us/area/county/1338'  
,'https://poweroutage.us/area/county/1476'
,'https://poweroutage.us/area/county/1364'
,'https://poweroutage.us/area/county/1354'    
,'https://poweroutage.us/area/county/1367'    
    ]


outage_df_list = []

for i in range(len(list_of_counties_website)):
    outage_df_list.append(power_outage_scrape(list_of_counties_website[i]))


outage_df1 = pd.concat(outage_df_list).reset_index()[['county','number_of_customers','number_of_outages','outage_percent','lastupdated']]
outage_df1
  