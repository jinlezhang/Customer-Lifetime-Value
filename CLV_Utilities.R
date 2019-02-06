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
library(SemiMarkov)
library(sqldf)


conn<-odbcConnect("PostgreSQL30")

elog<-sqlQuery(conn, "select cast(cast(a.cust_sa1 as bigint)/100 as int) sa2, 
                a.customer_id, a.tran_date_time, 
               sum(a.weighted_tran_amount) tran_amount 
               from base_westpac.transaction_data a
               inner join 
               (
               select cast(cast(cust_sa1 as bigint)/100 as int) sa2, tran_date_time from base_westpac.transaction_data where tran_month between '2017-11-01' and '2018-04-01' 
               and merchant_category_code=4900 and cust_state=1
               group by 1,2 having count(distinct customer_id) >=5 and count(distinct merchant_id)>=3
               ) b
               on cast(cast(a.cust_sa1 as bigint)/100 as int)=b.sa2 and a.tran_date_time=b.tran_date_time                
               where tran_month between '2017-11-01' and '2018-04-01' and merchant_category_code=4900 and cust_state=1
               group by 1,2,3")

elog$tran_month<-lubridate::month(elog$tran_date_time)

min_months<-elog %>% group_by(customer_id) %>% summarise(min_month=min(tran_month))

monthly_payers<-sqldf("select a.customer_id from elog a inner join min_months b on a.customer_id=b.customer_id and a.tran_month=b.min_month+1")

quarterly_payers<-sqldf("select b.customer_id first, a.customer_id last from min_months b left join elog a on a.customer_id=b.customer_id and a.tran_month=b.min_month+3")

elog$Freq<-ifelse(elog$customer_id %in% monthly_payers$customer_id | !elog$customer_id %in% quarterly_payers$last, "Monthly", "Quarterly")

elog$year<-lubridate::year(elog$tran_date_time)

elog_agg<-elog %>% group_by(customer_id, year, tran_month, Freq) %>% summarise(tran_amount=mean(tran_amount))





