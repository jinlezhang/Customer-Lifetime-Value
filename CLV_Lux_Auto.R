library(BTYD)

library(survival)
library(RODBC)
library(lubridate)
library(dplyr)
library(doParallel)
library(RSQLite)
library(sqldf)
library(data.table)
library(dplyr)
library(h2o)
library(quantmod)
library(lubridate)
library(stringr)
library(purrr)
library(tidyr)
library(tibble)

conn<-odbcConnect("PostgreSQL30")

elog<-sqlQuery(conn, "select cast(cast(a.cust_sa1 as bigint)/100 as int) sa2, 
                a.customer_id, a.tran_date_time, 
               sum(a.weighted_tran_amount) tran_amount
                from base_westpac.transaction_data a
                inner join 
               (
               select cast(cast(cust_sa1 as bigint)/100 as int) sa2, tran_date_time from base_westpac.transaction_data where tran_month between '2015-05-01' and '2018-05-01' and merchant_category_code=5511 
               group by 1,2 having count(distinct customer_id) >=5 and count(distinct merchant_id)>=3
                ) b
               on cast(cast(a.cust_sa1 as bigint)/100 as int)=b.sa2 and a.tran_date_time=b.tran_date_time    
                where tran_month between '2015-05-01' and '2018-05-01' and merchant_category_code=5511
                group by 1,2,3
                having sum(weighted_tran_amount)>60000")

elog<-elog[which(!is.na(elog$sa2)),]

# elog_sub<-elog %>% filter(sa2 %in% c(117031337, 117031333, 118011343, 118011346, 
#                                      118011340, 118011339, 118011341, 118021348, 
#                                      118011345, 117031336, 117031334,  117031331, 
#                                      117031332, 120031390, 120031575, 121041416,
#                                      121041415, 121011400, 117031330, 117031338))

elog_sub<-elog

elog_sub<-elog_sub[,-1]

elog_sub$tran_date_time<-as.Date(elog_sub$tran_date_time)

names(elog_sub)<-c("cust", "date", "sales")

elog_sub<-dc.MergeTransactionsOnSameDate(elog_sub)

min_date<-min(elog_sub$date)

max_date<-max(elog_sub$date)


end.of.cal.period<-as_date(min_date + as.duration((max_date-min_date)/2))

T.start<-as.numeric(max_date-end.of.cal.period)

T.tot<-end.of.cal.period

n.periods.final<-round(as.numeric(max_date-min_date)/7)

data<-dc.ElogToCbsCbt(elog_sub, per="week", T.cal = end.of.cal.period, statistic = "freq")

cal.cbs<-data[["cal"]][["cbs"]]

params<-pnbd.EstimateParameters(cal.cbs)

pnbd.PlotFrequencyInCalibration(params, cal.cbs, 10)

pnbd.PlotTrackingInc(params, T.cal, n.periods.final, w.track.data)


elog_1<-dc.SplitUpElogForRepeatTrans(elog_sub)$repeat.trans.elog

tot.cbt<-dc.CreateFreqCBT(elog_1)

cal.cbs_1<-cal.cbs %>% as_tibble(rownames='cust') %>% 
  mutate(x.star=map_int(cust, ~sum(.==elog_1$cust ))-x)

T.cal<-unlist(cal.cbs_1[,"T.cal"], use.names = FALSE)

names(T.cal)<-cal.cbs_1$cust

x.star<-cal.cbs_1$x.star

names(x.star)<-cal.cbs_1$cust

all_dates<-tibble(date=seq(min_date, max_date,1))

d.track.data<-tot.cbt %>% as_tibble() %>% mutate(date=as_date(date)) %>%
  right_join(all_dates, by="date") %>%
  replace_na(replace=list(cust=0,n=0)) %>% 
  group_by(date) %>% summarise(total_purchases=sum(n))

w.track.data<-d.track.data %>% mutate(week=as.numeric(date-min_date) %/% 7) %>%
  filter(week<n.periods.final) %>%
  group_by(week) %>% 
  summarise(by_week_total=sum(total_purchases)) %>%
  pull(by_week_total)

cum.track.data<-cumsum(w.track.data)

pnbd.PlotTrackingCum(params, T.cal, n.periods.final, cum.track.data, n.periods.final)

pnbd.PlotTrackingInc(params, T.cal, n.periods.final, w.track.data, n.periods.final)

heatmap.palive.data<-matrix(NA, nrow = 30, ncol=30)

heatmap.cet.data<-matrix(NA, nrow=30, ncol=30)

for (i in 1:30){
  heatmap.cet.data[i,]<-pnbd.ConditionalExpectedTransactions(params, T.star = 32,x=i,t.x = 1:30, T.cal = 13)
  
  heatmap.palive.data[i,]<-pnbd.PAlive(params, x=i, t.x=1:30, T.cal = 13)
}

image(heatmap.palive.data, axes=FALSE, xlab="Number of Transactions", 
      ylab="Weeks since last transaction", main="P(Alive) by Recency and Frequency")
axis(1, at=seq(0,1,.1), labels=seq(0,30,3))
axis(2, at=seq(0,1,.1), labels=seq(30,0,-3))

image(heatmap.cet.data, axes=FALSE, xlab="Number of Transactions", 
      ylab="Weeks since last transaction", main="Conditional Expected Transactions by Recency and Frequency")
axis(1, at=seq(0,1,.1), labels=seq(0,30,3))
axis(2, at=seq(0,1,.1), labels=seq(30,0,-3))


exp_transactions<-pnbd.Expectation(params, t=52)

purchases<-as_tibble(x.star) %>% 
  rownames_to_column() %>%
  select(cust=rowname, purchases=value)%>%
  arrange(desc(purchases))


mape<-function(actual, predicted){
  n<-length(actual)
  (1/n) * sum(abs((actual-predicted)/actual))
}

expected.cum.trasn<-pnbd.ExpectedCumulativeTransactions(params, T.cal, n.periods.final, n.periods.final)

end.of.cal.week<-round(as.numeric((end.of.cal.period - min(elog_1$date))/7))

total.mape<-mape(cum.track.data,expected.cum.trasn)

in.sample.mape<-mape(cum.track.data[1:end.of.cal.week], expected.cum.trasn[1:end.of.cal.week])

out.of.sample.mape<-mape(cum.track.data[end.of.cal.week:26], expected.cum.trasn[end.of.cal.week:26])




cal.cbt<-dc.ElogToCbsCbt(elog_sub, per="day", T.cal=end.of.cal.period, statistic = "total.spend")

cal.cbs_spend<-cal.cbt$cal$cbs

cal.cbt_spend<-cal.cbt$cal$cbt

calculateAvgSpend<-function(cbt.row){
  purchaseCols<-which(cbt.row!=0)
  sum(cbt.row[purchaseCols])/length(purchaseCols)
}

m.x<-apply(cal.cbt_spend, 1, calculateAvgSpend)

m.x[which(is.na(m.x))]<-0

spendParams<-spend.EstimateParameters(m.x, cal.cbs_spend[,1])

expected.spend<-spend.plot.average.transaction.value(spendParams, m.x, cal.cbs_spend[,1],
                                                     title="Actual vs. Expected Avg Transaction Value Across Customers")


end.of.cal.period.day<-as.Date(end.of.cal.period)

data_day<-dc.ElogToCbsCbt(elog_sub, per="day", T.cal = end.of.cal.period.day)

cal.cbs.day<-data_day[["cal"]][["cbs"]]

params<-pnbd.EstimateParameters(cal.cbs.day)

x<-cal.cbs.day[,"x"]

t.x<-cal.cbs.day[,"t.x"]

T.cal<-cal.cbs.day[,"T.cal"]

# Discount Rate
d<-0.15

dis_exp_transactions<-pnbd.DERT(params, x, t.x, T.cal, d/52)

expected_spend<-spend.expected.value(spendParams, m.x, cal.cbs.day[,1])

projected_future_revenue<-expected_spend * dis_exp_transactions


output<-data.frame(cust=data_day$cust.data$cust, dis_exp_transactions=dis_exp_transactions,
                   expected_spend=expected_spend, projected_future_revenue=projected_future_revenue)


elog_full<-unique(merge(elog[, 1:2], output, by.x="customer_id", by.y = "cust"))

CLV_by_SA2<-elog_full %>% group_by(sa2) %>% summarise(avg_CLV=mean(projected_future_revenue))

# CLV_by_SA2_20<-CLV_by_SA2 %>% filter(sa2 %in% c(117031337, 117031333, 118011343, 118011346, 
#                                               118011340, 118011339, 118011341, 118021348, 
#                                               118011345, 117031336, 117031334,  117031331, 
#                                               117031332, 120031390, 120031575, 121041416,
#                                               121041415, 121011400, 117031330, 117031338))

write.csv(CLV_by_SA2, "C:/dev/KZ/CLV_Lux_Auto_by_SA2.csv", row.names=FALSE)


