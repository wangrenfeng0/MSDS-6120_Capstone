library(odbc)
library(DBI)
library(tidyverse)
library(forecast)
library(tswge)
library(Metrics)
library(MLmetrics)
library(caret)
library(zoo)


############################################Import Data from SQL##############################################################


##connect to database
outage_data <- dbConnect(odbc(), 
                         Driver = "SQL Server", 
                         Server = "enter SQL SERVER name", #Enter SQL Server Name/IP address
                         Database = "enter database", #Enter Database name
                         UID ="enter username", #Enter user name
                         PWD = "enter password") #Enter password


outage_query <-dbSendQuery(outage_data, "
select 
[set_type] = (case when County in ('Harris','Tarrant','Dallas', 'Montgomery', 'Brazoria') then 'Train' else 'Validate' end)
,metro_area
 ,outage_bucket = (case 
  when outage_percent < .01 then '< 1%'
  when outage_percent between .01 and .03 then '1% - 3%'
 when outage_percent between .03 and .1 then '3% - 10%'
 when outage_percent > .1  then ' > 10%' end)
  ,outage_percent
  ,RecordDateTime_UTC
,County

  from (
  select 
  outage_percent = (case when Customer_Count = 0 then 0
  when Outage_Count > Customer_Count then 1
  else cast(Outage_Count as float)/cast(Customer_Count as float)  end)

  ,* from [power_outage_data].[dbo].[historic_weather_data_PIVOTED_with_ALL_FEATURES]
  ) x
  order by County, cast(RecordDateTime_UTC as datetime)
 ")

outage_data_raw <- dbFetch(outage_query)


############################################ARIMA Model Selection###############################################################


outage_train = outage_data_raw%>%filter(set_type=='Train')


#list of counties
outage_train%>%distinct(metro_area,County)


#AIC/BIC 5 selection function

ts.aic.funct = function(series,county='Harris')
{
  tseries1 <- ts(series%>%filter(County==county)%>%select('outage_percent'))
  tseries1_d <- artrans.wge(tseries1 ,phi.tr=1)
  
  aic5 <- aic5.wge(tseries1,p=0:15,q=0:5,type="aic")

  #bic5 <- aic5.wge(tseries1,p=0:15,q=0:5,type="bic")
  
  aic5_d <- aic5.wge(tseries1_d,p=0:15,q=0:5,type="aic")
  #bic5_d <- aic5.wge(tseries1_d,p=0:15,q=0:5,type="bic")
  
  return(list(aic5,aic5_d))
}



#get Houston AIC5 models
Harris_models <- ts.aic.funct(outage_train,county='Harris')
Brazoria_models <- ts.aic.funct(outage_train,county='Brazoria')
Montgomery_models <- ts.aic.funct(outage_train,county='Montgomery')


#get Dallas AIC5 models
Dallas_models <- ts.aic.funct(outage_train,county='Dallas')
Tarrant_models <- ts.aic.funct(outage_train,county='Tarrant')

#combine all AIC 5 models into list
all_aic_models <- list(Harris_models,Brazoria_models ,Montgomery_models ,Dallas_models,Tarrant_models)

#combine all time series data into list
all_timeseries <- list(
  ts(outage_train%>%filter(County=='Harris')%>%select('outage_percent'))
  ,ts(outage_train%>%filter(County=='Brazoria')%>%select('outage_percent'))
  ,ts(outage_train%>%filter(County=='Montgomery')%>%select('outage_percent'))
  ,ts(outage_train%>%filter(County=='Dallas')%>%select('outage_percent'))
  ,ts(outage_train%>%filter(County=='Tarrant')%>%select('outage_percent'))
  )



#Setup where models will be exported for later use
file_export <- choose.dir(default = "", caption = "choose where files will be exported")

#Save AIC 5 models
save(all_aic_models, file = paste(file_export,"\\all_aic_models.rda",sep='')) 

#Save Time series data
save(all_timeseries, file = paste(file_export,"\\all_timeseries.rda",sep='')) 

#load AIC models
load(paste(file_export,"\\all_aic_models.rda",sep=''))

#load Time series data
load(paste(file_export,"\\all_timeseries.rda",sep=''))



############################################ARIMA Model Building###############################################################

#Seperate out Houston Models and Time series
houston_aic_models <- all_aic_models[1:3]
houston_timeseries <- all_timeseries[1:3]

#Separare out Dallas Models and Time Series
dallas_aic_models <- all_aic_models[4:5]
dallas_timeseries <- all_timeseries[4:5]


#function to build ARIMA Models, stores results in list by time series realization, first list is ARMA models, second list is ARIMA models with differncing = 1

ts.arma.funct = function(aic_models,time_series)
{
  all_fits_list = list()

  
  for( i in 1:length(aic_models))
  {
    
    ARMA_list = list()
    ARIMA_list = list()
    
    model1 <- aic_models[[i]]
    ts1 <- time_series[[i]]
    ts1_d <- artrans.wge(ts1 ,phi.tr=1)
    
    model1_ARMA <- model1[[1]]
    model1_ARIMA <- model1[[2]]
    
    
    for (j in 1:5)
    {
      
      p1 = model1_ARMA[j,1]
      q1 = model1_ARMA[j,2]
      
      p1_d = model1_ARIMA[j,1]
      q1_d = model1_ARIMA[j,2]
      
      ARMA_fit <- est.arma.wge(ts1,p=p1,q=q1)
      
      ARIMA_fit <- est.arma.wge(ts1_d,p=p1_d,q=q1_d)
      
      #ARMA_list <- append(ARMA_list, ARMA_fit)
      #ARIMA_list <- append(ARIMA_list, ARIMA_fit)
      
      ARMA_list[[j]] <- ARMA_fit
      ARIMA_list[[j]] <- ARIMA_fit

    }
    
    #all_fits_list <- append(all_fits_list,list(ARMA_list,ARIMA_list))   
    all_fits_list[[i]] <- list(ARMA_list,ARIMA_list)
    
  } 
  
    
    return(all_fits_list) 
}
    


#fitted Houston Models
Houston_fits <- ts.arma.funct(houston_aic_models,houston_timeseries)

#fitted Dallas Models
Dallas_fits <- ts.arma.funct(dallas_aic_models ,dallas_timeseries)


#Save Houston fits
save(Houston_fits, file = paste(file_export,"\\Houston_fits.rda",sep='')) 

#Save Dallas fits
save(Dallas_fits, file = paste(file_export,"\\Dallas_fits.rda",sep='')) 


#load Houston fits
load(paste(file_export,"\\Houston_fits.rda",sep=''))

#load Dallas fits
load(paste(file_export,"\\Dallas_fits.rda",sep=''))




####combine model fits into a list, differencing factor then ARMA coefficients
ts.d_model_list.funct = function(model_fits)
{

model_fits2 = list()

for (j in 1:length(model_fits))
{
  
  for (k in 1:1) #changed from 1:5 to 1:1 to limit model training
  {
  arma1 <- model_fits[[j]][[1]][[k]]
  d <- 0
  d_arma <- list(d,arma1)
  model_fits2[[length(model_fits2) + 1]] <- d_arma 
  }
  
}


for (j in 1:length(model_fits))
{
  
  for (k in 1:1) #changed from 1:5 to 1:1 to limit model training
  {
    arma1 <- model_fits[[j]][[2]][[k]]
    d <- 1
    d_arma <- list(d,arma1)
    model_fits2[[length(model_fits2) + 1]] <- d_arma 
  }
  
}


return(model_fits2) 

}



Houston_fits2 <- ts.d_model_list.funct(Houston_fits)

Dallas_fits2 <- ts.d_model_list.funct(Dallas_fits)




#Save Houston fits2
save(Houston_fits2, file = paste(file_export,"\\Houston_fits2.rda",sep='')) 

#Save Dallas fits2
save(Dallas_fits2, file = paste(file_export,"\\Dallas_fits2.rda",sep='')) 


#load Houston fits2
load(paste(file_export,"\\Houston_fits2.rda",sep=''))

#load Dallas fits2
load(paste(file_export,"\\Dallas_fits2.rda",sep=''))



############################################ARIMA Model Training###############################################################


ts.model_forecast.funct = function(ts_model,list_of_ts)
{

  
d1 <- ts_model[[1]]

phi1 <- ts_model[[2]]$phi

theta1 <- ts_model[[2]]$theta

ts_actuals <- c()
ts_forecasts <- c()


ts_len <- length(list_of_ts)

for (i in 1:ts_len)
{


ts1 <- list_of_ts[[i]]

lagterms <- max(length(phi1),length(theta1))+d1

lag_len <- length(ts1)- lagterms


for (j in 1:lag_len)
{

forcast_1 <- fore.aruma.wge(ts1[(1+(j-1)):(lagterms+2+(j-1))],phi = phi1, theta = theta1, s = 0, d = d1,n.ahead = 2,lastn=TRUE,plot = FALSE)


ts_actuals <- c(ts_actuals, ts1[lagterms+(j-1)+1] )

ts_forecasts <- c(ts_forecasts,forcast_1$f[1] )

}

}

result_df <- data.frame(ts_actuals, ts_forecasts) 

result_df2 <- result_df %>% 
  mutate(actuals_bucket = factor(case_when(
    ts_actuals < .01 ~ "< 1%",
    ts_actuals >= .01 & ts_actuals < .03 ~ "1% - 3%",
    ts_actuals >= .03 & ts_actuals < .1 ~ "3% - 10",
    ts_actuals >= .1 ~ "> 10%"
  ),levels=c("< 1%","1% - 3%","3% - 10","> 10%")),
  forecasts_bucket = factor(case_when(
    ts_forecasts < .01 ~ "< 1%",
    ts_forecasts >= .01 & ts_forecasts < .03 ~ "1% - 3%",
    ts_forecasts >= .03 & ts_forecasts < .1 ~ "3% - 10",
    ts_forecasts >= .1 ~ "> 10%" ),levels=c("< 1%","1% - 3%","3% - 10","> 10%"))
  )

result_df2 <- tibble::rowid_to_column(result_df2, "ID")

result_df2 <- na.locf(na.locf(result_df2),fromLast=TRUE)


return(
list(result_df2,rmse(result_df2$ts_actuals, result_df2$ts_forecasts),R2_Score(result_df2$ts_forecasts,result_df2$ts_actuals),confusionMatrix(factor(result_df2$forecasts_bucket),factor(result_df2$actuals_bucket)))  
  
)

}


#########Run Metrics for all training Set
Houston1_results <- ts.model_forecast.funct(Houston_fits2[[1]],houston_timeseries)
Houston2_results <- ts.model_forecast.funct(Houston_fits2[[2]],houston_timeseries)
Houston3_results <- ts.model_forecast.funct(Houston_fits2[[3]],houston_timeseries)
Houston4_results <- ts.model_forecast.funct(Houston_fits2[[4]],houston_timeseries)
Houston5_results <- ts.model_forecast.funct(Houston_fits2[[5]],houston_timeseries)
Houston6_results <- ts.model_forecast.funct(Houston_fits2[[6]],houston_timeseries)

Houston_All_Results <- list(Houston1_results,Houston2_results,Houston3_results,Houston4_results,Houston5_results,Houston6_results)


Dallas1_results <- ts.model_forecast.funct(Dallas_fits2[[1]],dallas_timeseries)
Dallas2_results <- ts.model_forecast.funct(Dallas_fits2[[2]],dallas_timeseries)
Dallas3_results <- ts.model_forecast.funct(Dallas_fits2[[3]],dallas_timeseries)
Dallas4_results <- ts.model_forecast.funct(Dallas_fits2[[4]],dallas_timeseries)


Dallas_All_Results <- list(Dallas1_results,Dallas2_results,Dallas3_results,Dallas4_results)



#Save Houston_All_Results
save(Houston_All_Results, file = paste(file_export,"\\Houston_All_Results.rda",sep='')) 

#Save Dallas fits2
save(Dallas_All_Results, file = paste(file_export,"\\Dallas_All_Results.rda",sep='')) 


#load Houston fits2
load(paste(file_export,"\\Houston_All_Results.rda",sep=''))

#load Dallas fits2
load(paste(file_export,"\\Dallas_All_Results.rda",sep=''))


#####Function showing all metrics, ranking of metrics
ts.model_metrics_df.funct = function(list_of_results)
{

model_no <- c()
RMSE <- c()
R2 <- c()
Ovr_Acc <- c()
Bal_Acc_C1 <- c()
Bal_Acc_C2 <- c()
Bal_Acc_C3 <- c()
Bal_Acc_C4 <- c()
F1_C1 <- c()
F1_C2 <- c()
F1_C3 <- c()
F1_C4 <- c()

for (j in 1:length(list_of_results))
{
  
  model_no <- c(model_no,j)
  RMSE <- c(RMSE,list_of_results[[j]][[2]])
  
  R2 <- c(R2,list_of_results[[j]][[3]])  
  
  Ovr_Acc <- c(Ovr_Acc,list_of_results[[j]][[4]][[3]][[1]])

  Bal_Acc_C1 <- c(Bal_Acc_C1,list_of_results[[j]][[4]][[4]][1,11])
  Bal_Acc_C2 <- c(Bal_Acc_C2,list_of_results[[j]][[4]][[4]][2,11])
  Bal_Acc_C3 <- c(Bal_Acc_C3,list_of_results[[j]][[4]][[4]][3,11])
  Bal_Acc_C4 <- c(Bal_Acc_C4,list_of_results[[j]][[4]][[4]][4,11])
  
  F1_C1 <- c(F1_C1,list_of_results[[j]][[4]][[4]][1,7])
  F1_C2 <- c(F1_C2,list_of_results[[j]][[4]][[4]][2,7])
  F1_C3 <- c(F1_C3,list_of_results[[j]][[4]][[4]][3,7])
  F1_C4 <- c(F1_C4,list_of_results[[j]][[4]][[4]][4,7])
  
  
}

RMSE_rank = rank(RMSE)
R2_rank = rank(-R2)
Ovr_Acc_rank = rank(-Ovr_Acc)
Bal_Acc_C1_rank = rank(-Bal_Acc_C1)
Bal_Acc_C2_rank = rank(-Bal_Acc_C2)
Bal_Acc_C3_rank = rank(-Bal_Acc_C3)
Bal_Acc_C4_rank = rank(-Bal_Acc_C4)
F1_C1_rank = rank(-F1_C1)
F1_C2_rank = rank(-F1_C2)
F1_C3_rank = rank(-F1_C3)
F1_C4_rank = rank(-F1_C4)



metrics_df <- data.frame(model_no,RMSE,R2,Ovr_Acc,Bal_Acc_C1,Bal_Acc_C2,Bal_Acc_C3,Bal_Acc_C4,F1_C1,F1_C2,F1_C3,F1_C4)

rank_df <- data.frame(model_no,RMSE_rank,R2_rank,Ovr_Acc_rank,Bal_Acc_C1_rank,Bal_Acc_C2_rank,Bal_Acc_C3_rank,Bal_Acc_C4_rank,F1_C1_rank,F1_C2_rank,F1_C3_rank,F1_C4_rank)


avg_rank_df <- arrange(
  as.data.frame(rank_df %>%
                  pivot_longer(!model_no, names_to = "rank_type", values_to = "rank")%>%
                  group_by(model_no) %>%summarise(avg_rank = mean(rank))),avg_rank)


metrics_list <- list(metrics_df,rank_df,avg_rank_df)

return(metrics_list)

}



Houston_Metrics_DF <- ts.model_metrics_df.funct(Houston_All_Results)


Dallas_Metrics_DF <- ts.model_metrics_df.funct(Dallas_All_Results)


#Save Houston_Metrics_DF
save(Houston_Metrics_DF, file = paste(file_export,"\\Houston_Metrics_DF.rda",sep='')) 

#Save Dallas_Metrics_DF
save(Dallas_Metrics_DF, file = paste(file_export,"\\Dallas_Metrics_DF.rda",sep='')) 


#load Houston_Metrics_DF
load(paste(file_export,"\\Houston_Metrics_DF.rda",sep=''))

#load Dallas_Metrics_DF
load(paste(file_export,"\\Dallas_Metrics_DF.rda",sep=''))


############################################ARIMA Validation Set###############################################################

Houston_best_model <- Houston_Metrics_DF[[3]][1,1]

Dallas_best_model <- Dallas_Metrics_DF[[3]][1,1]


outage_validate = outage_data_raw%>%filter(set_type=='Validate')

outage_validate%>%distinct(metro_area,County)



validate_timeseries <- list(
  ts(outage_validate%>%filter(County=='Fort Bend')%>%select('outage_percent'))
  ,ts(outage_validate%>%filter(County=='Galveston')%>%select('outage_percent'))
  ,ts(outage_validate%>%filter(County=='Collin')%>%select('outage_percent'))
  ,ts(outage_validate%>%filter(County=='Denton')%>%select('outage_percent'))
)


houston_validate_ts <- validate_timeseries[1:2]
dallas_validate_ts <- validate_timeseries[3:4]




Houston1_Validation_results <- ts.model_forecast.funct(Houston_fits2[[Houston_best_model]],houston_validate_ts)

Dallas1_Validation_results <- ts.model_forecast.funct(Dallas_fits2[[Dallas_best_model]],dallas_validate_ts)



Houston1_Validation_metrics <- ts.model_metrics_df.funct(list(Houston1_Validation_results))

Dallas1_Validation_metrics <- ts.model_metrics_df.funct(list(Dallas1_Validation_results))




#Save Houston1_Validation_results
save(Houston1_Validation_results, file = paste(file_export,"\\Houston1_Validation_results.rda",sep='')) 

#Save Dallas1_Validation_results 
save(Dallas1_Validation_results , file = paste(file_export,"\\Dallas1_Validation_results.rda",sep='')) 

#Houston1_Validation_metrics
save(Houston1_Validation_metrics, file = paste(file_export,"\\Houston1_Validation_metrics.rda",sep='')) 

#Save Dallas1_Validation_metrics
save(Dallas1_Validation_metrics , file = paste(file_export,"\\Dallas1_Validation_metrics.rda",sep='')) 







#load Houston1_Validation_results
load(paste(file_export,"\\Houston1_Validation_results.rda",sep=''))


#load Dallas1_Validation_results
load(paste(file_export,"\\Dallas1_Validation_results.rda",sep=''))


#load Houston1_Validation_metrics
load(paste(file_export,"\\Houston1_Validation_metrics.rda",sep=''))


#load Dallas1_Validation_metrics
load(paste(file_export,"\\Dallas1_Validation_metrics.rda",sep=''))




