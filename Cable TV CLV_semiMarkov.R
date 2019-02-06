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
library(ggplot2)
library(plotly)
library(SemiMarkov)
library(onehot)
library(caret)
library(SMM)
library(msm)

conn<-odbcConnect("PostgreSQL30")

data<-sqlQuery(conn, "SELECT 
               customer_id, age_band, gender,
               tran_month,
               cust_sa1,
               datediff(day, cust_valid_from, tran_month) Customer_Duration,
               sum(case when merchant_category_code=4899 then weighted_tran_amount end) Cabel_TV_tran_amount,
               sum(case when merchant_category_code=4814 then weighted_tran_amount end) Telecom_tran_amount,
                sum(case when merchant_category_code=4812 then weighted_tran_amount end) Telecom_Equipment_tran_amount,
                sum(case when merchant_category_code=4816 then weighted_tran_amount end) Network_tran_amount,
                sum(case when merchant_category_code=5411 then weighted_tran_amount end) Grocery_tran_amount,
                sum(case when merchant_category_code=4900 then weighted_tran_amount end) Utilities_tran_amount,
                sum(case when merchant_category_code=8211 then weighted_tran_amount end) School_tran_amount,
                sum(case when merchant_category_code=5511 then weighted_tran_amount end) Auto_tran_amount
               FROM base_westpac.transaction_data where 
               tran_month between '2017-11-01' and '2018-04-01' and cust_state=1
               group by 1,2,3,4,5,6")


data[is.na(data)]<-0

percentiles<-c()
for (i in seq(0.1,0.9,0.1)) {
  percentiles<-c(percentiles, quantile(data$cabel_tv_tran_amount[data$cabel_tv_tran_amount>0],i))
}

percentiles<-unique(percentiles)

data$Cabel_TV_Band<-ifelse(data$cabel_tv_tran_amount>percentiles[8], 1, ifelse(data$cabel_tv_tran_amount>percentiles[7],2,
                                                                               ifelse(data$cabel_tv_tran_amount>percentiles[6],3,
                                                                                      ifelse(data$cabel_tv_tran_amount>percentiles[5],4,
                                                                                             ifelse(data$cabel_tv_tran_amount>percentiles[4],5,
                                                                                                    ifelse(data$cabel_tv_tran_amount>percentiles[3],6,
                                                                                                           ifelse(data$cabel_tv_tran_amount>percentiles[2],7,8)))))))

a<-data %>% group_by(customer_id) %>% summarise(count=n_distinct(Cabel_TV_Band))

a %>% group_by(count) %>% summarise(n())

customer_ids_index<-data.frame(customer_id=unique(data$customer_id), row=1:length(unique(data$customer_id)))

data<-merge(customer_ids_index, data, by="customer_id")

data<-data[,-1]

data<-data[,-7]

cl<-makeCluster(10)
registerDoParallel(cl)

results<-foreach(i=unique(data$row), .packages = "dplyr") %dopar% {
  df<-data %>% filter(row==i) %>% arrange(tran_month)
  
  n<-0
  if (nrow(df)>1){
    
    for (x in 2:nrow(df)){
      if(df$Cabel_TV_Band[x]==df$Cabel_TV_Band[x-1]){
        n<-n+1
      }
    }
    
    rows<-nrow(df)-n
    
    df1<-data.frame(row=rep(i,rows), start=rep(0,rows), end=rep(0,rows), time=rep(1,rows), month=rep(df$tran_month, rows))
   
    k<-1
    start<-df$Cabel_TV_Band[1]
    m<-1
  
    for (j in 2:nrow(df)){
      if (df$Cabel_TV_Band[j]!=start){
        df1$start[k]<-df$Cabel_TV_Band[j-1]
        df1$end[k]<-df$Cabel_TV_Band[j]
        df1$time[k]<-m
        k<-k+1
      } else {
        df1$time[k]<-df1$time[k]+1
        m<-m+1
      }
      start<-df$Cabel_TV_Band[j]
    }
    
    df2<-merge(df1, df, by="row")
    df3<-df2 %>% filter(month==tran_month)
    # df4<-df3 %>% group_by(row, start, end,time, age_band, gender, month) %>% summarise(customer_duration=mean(customer_duration), telecom_tran_amount=mean(telecom_tran_amount),
    #                                                           telecom_equipment_tran_amount=mean(telecom_equipment_tran_amount),
    #                                                           network_tran_amount=mean(network_tran_amount), grocery_tran_amount=mean(grocery_tran_amount),
    #                                                           utilities_tran_amount=mean(utilities_tran_amount), school_tran_amount=mean(school_tran_amount),
    #                                                           auto_tran_amount=mean(auto_tran_amount))
    # df4<-as.data.frame(cbind(unique(df3[,c(1:5,6:7)]), df4))
    if(rows>1 && df3[rows, 2]==0 && df3[(rows-1), 3]>0) {
      df3[rows, 2:3]<-df3[(rows-1), 3]
      df3[rows, 4]<-1
    }
    df3[1:rows,]
  }
}


stopCluster(cl)

results_df<-rbindlist(results, fill = TRUE)

table.state(results_df)

states<-c("0","1","2","3","4","5")

mtrans<-matrix(FALSE, nrow=6, ncol=6)

mtrans[1,2:6]<-rep("E",5)

mtrans[2,c(1,3:6)]<-rep("E",5)

mtrans[3,c(1:2,4:6)]<-rep("E",5)

mtrans[4,c(1:3,5:6)]<-rep("E",5)

mtrans[5,c(1:4,6)]<-rep("E",5)

mtrans[6,1:5]<-rep("E",5)

cov<-results_df[,c(6:7, 10:17)]

selected_rows<-c()
for(i in 0:5){
  df<-results_df %>% filter(Cabel_TV_Band==i)
  selected_rows<-c(selected_rows, unique(df$row[1:100]))
}

selected_rows<-selected_rows[!is.na(selected_rows)]

input_data<-results_df[selected_rows,1:4]

names(input_data)<-c("id", "state.h", "state.j", "time")

cov<-cov[selected_rows,-c(1:2)]

fit<-semiMarkov(data=input_data, states=states, mtrans=mtrans, cov=cov, 
                dist_init=c(rep(1.5,30),rep(1.8,30)), 
                proba_init=rep(0.1, 30))

inEqualLB<-list(c("coef",1,-5), c("coef",2,-5),c("coef",3,-5),c("coef",4,-5),c("coef",5,-5),
                c("coef",6,-5),c("coef",7,-5),c("coef",8,-5),c("coef",9,-5),c("coef",10,-5),
                c("coef",11,-5),c("coef",12,-5),c("coef",13,-5),c("coef",14,-5),c("coef",15,-5),
                c("coef",16,-5),c("coef",17,-5),c("coef",18,-5),c("coef",19,-5),c("coef",20,-5),
                c("coef",21,-5),c("coef",22,-5),c("coef",23,-5),c("coef",24,-5),c("coef",25,-5),
                c("coef",26,-5),c("coef",27,-5),c("coef",28,-5),c("coef",29,-5),c("coef",30,-5))

inEqualUB<-list(c("coef",1,5), c("coef",2,5),c("coef",3,5),c("coef",4,5),c("coef",5,5),
                c("coef",6,5),c("coef",7,5),c("coef",8,5),c("coef",9,5),c("coef",10,5),
                c("coef",11,5),c("coef",12,5),c("coef",13,5),c("coef",14,5),c("coef",15,5),
                c("coef",16,5),c("coef",17,5),c("coef",18,5),c("coef",19,5),c("coef",20,5),
                c("coef",21,5),c("coef",22,5),c("coef",23,5),c("coef",24,5),c("coef",25,5),
                c("coef",26,5),c("coef",27,5),c("coef",28,5),c("coef",29,5),c("coef",30,5))


fit<-semiMarkov(data=input_data, states=states, mtrans=mtrans, cov=cov, 
                dist_init=c(rep(1.5,30)), 
                proba_init=rep(0.01, 30), ineqLB=inEqualLB, ineqUB = inEqualUB)







