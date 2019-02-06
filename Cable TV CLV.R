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
library(FinCal)
library(dbplyr)
library(DBI)


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
                                                                                                           ifelse(data$cabel_tv_tran_amount>percentiles[2],7,
                                                                                                                  ifelse(data$cabel_tv_tran_amount>percentiles[1],8,
                                                                                                                         ifelse(data$cabel_tv_tran_amount>0,9,10)))))))))

a<-data %>% group_by(customer_id) %>% summarise(count=n_distinct(Cabel_TV_Band))

a %>% group_by(count) %>% summarise(n())

customer_ids_index<-data.frame(customer_id=unique(data$customer_id), row=1:length(unique(data$customer_id)))

data<-merge(customer_ids_index, data, by="customer_id")

data<-data[,-1]

cl<-makeCluster(10)
registerDoParallel(cl)

results<-foreach(i=unique(data$row), .packages = "dplyr") %dopar% {
  df<-data %>% filter(row==i) %>% arrange(tran_month)
  
  dates<-data.frame(tran_month=seq(as.Date("2017-11-01"), as.Date("2018-04-01"), by="month"))
  
  df<-merge(df,dates, by= "tran_month", all.y = TRUE)
  
  df$firstobs<-c(1, rep(0, nrow(df)-1))
  df$months<-1:6
  df$row[is.na(df$row)]<-i
  df$age_band[is.na(df$age_band)]<-unique(df$age_band[!is.na(df$age_band)])
  df$gender[is.na(df$gender)]<-unique(df$gender[!is.na(df$gender)])
  df$cust_sa1[is.na(df$cust_sa1)]<-unique(df$cust_sa1[!is.na(df$cust_sa1)])
  df
}

stopCluster(cl)

results_df<-rbindlist(results, fill = TRUE)

table.state(results_df)

state_table<-statetable.msm(Cabel_TV_Band, row, data=results_df)

state_matrix<-t(matrix(unlist(as.data.frame(state_table)[,3]), nrow=10, byrow = TRUE))

row_sum<-apply(state_matrix, 1, sum)

for (i in 1:10){
  state_matrix[i,]<-state_matrix[i,]/row_sum[i]
}

rownames(state_matrix)<-colnames(state_matrix)<-c("Tier 1", "Tier 2","Tier 3","Tier 4","Tier 5",
                                                  "Tier 6","Tier 7","Tier 9","Tier 10","Tier 11")

results_df$Cabel_TV_Band[is.na(results_df$Cabel_TV_Band)]<-11

results_df[is.na(results_df)]<-0

results_df$Cabel_TV_Band[results_df$Cabel_TV_Band==9]<-8

results_df$Cabel_TV_Band[results_df$Cabel_TV_Band==10]<-9

results_df$Cabel_TV_Band[results_df$Cabel_TV_Band==11]<-10

results_df<-as.data.frame(results_df)

# data_input<-results_df[,-c(1,5,7)] %>% filter(row<=5)

for (i in c(6,8:14)){
  results_df[,i]<-ifelse(sum(results_df[,i])==0,0,(results_df[,i]-mean(results_df[,i]))/(max(results_df[,i])-min(results_df[,i])))
}

msm_model<-msm(Cabel_TV_Band ~ months, subject = row, data=results_df[,c(1,2,6,8:17)], qmatrix = state_matrix,
               deathexact = FALSE, covariates = ~ customer_duration + telecom_tran_amount + 
                 telecom_equipment_tran_amount + network_tran_amount + grocery_tran_amount + 
                 utilities_tran_amount + school_tran_amount + auto_tran_amount, control=list(fnscale=4000))

avg_by_tier<-results_df %>% group_by(Cabel_TV_Band) %>% summarise(avg_by_tier=median(cabel_tv_tran_amount))

avg_by_tier_customer<-results_df %>% group_by(row, Cabel_TV_Band) %>% summarise(avg_by_tier_customer=median(cabel_tv_tran_amount))

all_rows<-data.frame(row=unique(results_df$row))

# all_bands<-data.frame(bands=1:10)

# all_bands_rows<-merge(all_rows, all_bands, all = TRUE)

avg_by_tier<-merge(all_rows, avg_by_tier, all=TRUE)

avg_all<-merge(avg_by_tier_customer, avg_by_tier, by=c("row", "Cabel_TV_Band"), all.y = TRUE)

# avg_all<-merge(all_bands_rows, avg_all, by.y=c("row","Cabel_TV_Band"), by.x =c("row", "bands"), all.x = TRUE)

avg_all$avg_by_tier_customer<-ifelse(is.na(avg_all$avg_by_tier_customer), avg_all$avg_by_tier, avg_all$avg_by_tier_customer)

# Rouse Hill - McGraths Hill,Sydney Inner City,Eastern Suburbs - North,
# Eastern Suburbs - South,Strathfield - Burwood - Ashfield,North Sydney - Mosman,Manly,Parramatta,
# Ryde - Hunters Hill
rows_central<-unique(data$row[as.integer(data$cust_sa1/100) %in% c(115041301,115041302,117031329,117031330,117031331,117031332,117031333,117031334,117031335,117031336,117031337,117031338,118011339,118011340,118011341,118011342,118011343,118011344,118011345,118011346,118011347,118021348,118021350,118021564,118021565,118021566,118021567,118021568,118021569,118021570,120031390,120031391,120031392,120031393,120031394,120031395,120031396,120031575,120031576,121041413,121041414,121041415,121041416,121041417,122011418,122011419,125041489,125041490,125041491,125041492,125041493,125041494,125041588,125041589,126021497,126021498,126021499,126021500,126021501,126021503,126021590,126021591
)])

# Sydney inner city
rows_central<-unique(data$row[as.integer(data$cust_sa1/100) %in% c(117031329,117031330,117031331,117031332,117031333,117031334,117031335,117031336,117031337,117031338
)])

# Greater Sydney
rows_central<-unique(data$row[as.integer(data$cust_sa1/100) %in% c(102011028,102011029,102011030,102011031,102011032,102011033,102011034,102011035,102011036,102011037,102011038,102011039,102011040,102011041,102011042,102011043,102021044,102021045,102021046,102021047,102021048,102021049,102021050,102021051,102021052,102021053,102021054,102021055,102021056,102021057,115011290,115011291,115011294,115011296,115011553,115011554,115011555,115011556,115011557,115011558,115011559,115021297,115021298,115031299,115031300,115041301,115041302,116011303,116011304,116011306,116011307,116011308,116011560,116011561,116021309,116021310,116021312,116021562,116021563,116031313,116031314,116031315,116031316,116031317,116031318,116031319,117011320,117011321,117011322,117011323,117011324,117011325,117021326,117021327,117021328,117031329,117031330,117031331,117031332,117031333,117031334,117031335,117031336,117031337,117031338,118011339,118011340,118011341,118011342,118011343,118011344,118011345,118011346,118011347,118021348,118021350,118021564,118021565,118021566,118021567,118021568,118021569,118021570,119011354,119011355,119011356,119011357,119011358,119011359,119011360,119011361,119011571,119011572,119021362,119021363,119021364,119021366,119021367,119021573,119021574,119031368,119031369,119031370,119031371,119031372,119031373,119031374,119041375,119041376,119041377,119041378,119041379,119041380,119041381,119041382,120011383,120011384,120011385,120011386,120021387,120021388,120021389,120031390,120031391,120031392,120031393,120031394,120031395,120031396,120031575,120031576,121011398,121011399,121011400,121011401,121011402,121021403,121021404,121021406,121021577,121021578,121021579,121031407,121031408,121031409,121031410,121031411,121031412,121041413,121041414,121041415,121041416,121041417,122011418,122011419,122021420,122021421,122021422,122021423,122031424,122031425,122031426,122031427,122031428,122031429,122031430,122031431,122031432,123011433,123011434,123011435,123021436,123021437,123021438,123021439,123021440,123021441,123021442,123021443,123021444,123031445,123031446,123031447,123031448,124011449,124011450,124011451,124011452,124011453,124011454,124011455,124021456,124031457,124031458,124031459,124031460,124031461,124031462,124031463,124031464,124031465,124041466,124041467,124041468,124051469,124051470,124051580,124051581,125011473,125011475,125011582,125011583,125011584,125011585,125011586,125011587,125021476,125021477,125021478,125031479,125031480,125031481,125031482,125031483,125031484,125031485,125031486,125031487,125041489,125041490,125041491,125041492,125041493,125041494,125041588,125041589,126011495,126011496,126021497,126021498,126021499,126021500,126021501,126021503,126021590,126021591,127011504,127011505,127011506,127011592,127011593,127011594,127011595,127011596,127011597,127021509,127021510,127021511,127021512,127021513,127021514,127021515,127021516,127021517,127021518,127021519,127021520,127021521,127031522,127031523,127031524,127031598,127031599,127031600,127031601,128011529,128011530,128011531,128011602,128011603,128011604,128011605,128011606,128021533,128021534,128021535,128021536,128021537,128021538,128021607,128021608,128021609
)])

avg_subset<-avg_all %>% filter(row %in% rows_central)

current_transition_matrix<-pmatrix.msm(msm_model,t=1)

base_matrix<-matrix(c(rep(current_transition_matrix[1,1],10), 
                      rep(current_transition_matrix[2,2],10),
                      rep(current_transition_matrix[3,3],10),
                      rep(current_transition_matrix[4,4],10),
                      rep(current_transition_matrix[5,5],10),
                      rep(current_transition_matrix[6,6],10),
                      rep(current_transition_matrix[7,7],10),
                      rep(current_transition_matrix[8,8],10),
                      rep(current_transition_matrix[9,9],10),
                      rep(current_transition_matrix[10,10],10)), nrow=10, byrow = TRUE)

current_transition_matrix<--current_transition_matrix/ (1+0.02/12-base_matrix)

diag(current_transition_matrix)<-1

data_list<-list()
data_list[[1]]<-avg_subset %>% filter(row %in% unique(avg_subset$row)[1:398478])
data_list[[2]]<-avg_subset %>% filter(row %in% unique(avg_subset$row)[398479:796956])
data_list[[3]]<-avg_subset %>% filter(row %in% unique(avg_subset$row)[796957:1195435])


self_transisions<-c()
transition_matrix<-pmatrix.msm(msm_model,t=1)

for (k in 1:10){
  self_transisions<-c(self_transisions, transition_matrix[k,k]/(1-transition_matrix[k,k]+0.02/12))
}

CLV_df_list<-list()

j<-1

for (d in data_list){
  
  cl<-makeCluster(6)
  registerDoParallel(cl)
  
  CLV_result<-foreach(i = unique(d$row), .packages = c("dplyr", "msm", "FinCal")) %dopar% {
    avg_values<-d$avg_by_tier_customer[which(d$row==i)]
    
    self_transisions1<-self_transisions * avg_values
    
    # for (k in 1:10){
    #   j<-2
    #   while (min(transition_matrix[,10])<0.8){
    #     transition_matrix<-pmatrix.msm(msm_model,t=j)
    #     probs<-c(probs, transition_matrix[k,k])
    #     j<-j+1
    #   }
    #   self_transisions<-c(self_transisions, npv(0.02/12, c(0, avg_values[k] * probs)))
    # }
    
    CLV<-solve(current_transition_matrix, self_transisions1)
    
    df1<-as.data.frame(t(data.frame(CLV=CLV)))
    df1$row<-i
    names(df1)[8:10]<-c("Tier 8", "Tier 9", "Tier 10")
    df1
  }
  
  CLV_df_list[[j]]<-rbindlist(CLV_result,fill=TRUE)
  
  j<-j+1
  stopCluster(cl)
}

CLV_df<-rbindlist(CLV_df_list, fill=TRUE)

CLV_df<-unique(merge(CLV_df, data[,c(1,5)], by="row"))

CLV_df$cust_sa2<-as.integer(CLV_df$cust_sa1/100)

last_state<-data[which(data$tran_month=="2018-04-01"),c("row", "Cabel_TV_Band")]

last_state$Cabel_TV_Band<-ifelse(last_state$Cabel_TV_Band==9,8,ifelse(last_state$Cabel_TV_Band==10,9,
                                                                      ifelse(last_state$Cabel_TV_Band==11,10,
                                                                      last_state$Cabel_TV_Band)))

CLV_df<-merge(CLV_df, last_state, by="row")

CLV_df<-as.data.frame(CLV_df)

CLV_df<-merge(CLV_df, customer_ids_index, by="row")

CLV_df_value_only<-CLV_df[,15:14]

intial_state_CLV<-c()
for (i in 1:nrow(CLV_df)){
  intial_state_CLV<-c(intial_state_CLV, CLV_df[i,CLV_df$Cabel_TV_Band[i]+1])
}

CLV_df$Current_CLV<-intial_state_CLV

CLV_SA2<-unique(unique(CLV_df) %>% group_by(cust_sa2) %>% summarise(Total_CLV=sum(Current_CLV)))

write.csv(CLV_SA2,"C:/dev/KZ/CLV_SA2_Pay_TV.csv")

CLV_df$Month<-as.Date("2018-04-01")

CLV_df$mcc<-4899

CLV_df[is.na(CLV_df)]<-0

sqlSave(conn, dat=CLV_df, "CLV", rownames = FALSE, append = TRUE)

CLV_inital_state<-unique(CLV_df[,15:16])

df_transition<-data.frame(a=rep(0,10))
for ( i in 1:10){
  df_transition<-cbind(df_transition, transition_matrix[,i])
}

df_transition<-df_transition[,-1]

names(df_transition)<-c("Tier 1", "Tier 2","Tier 3","Tier 4","Tier 5",
                        "Tier 6","Tier 7","Tier 8","Tier 9","Tier 10")

rownames(df_transition)<-c("Tier 1", "Tier 2","Tier 3","Tier 4","Tier 5",
                           "Tier 6","Tier 7","Tier 8","Tier 9","Tier 10")

spend_by_band<-data%>% group_by(Cabel_TV_Band) %>% summarise(Median_Spend=median(cabel_tv_tran_amount))

spend_by_band$Cabel_TV_Band<-1:10

names(spend_by_band)[1]<-"Tier"

harzards<-hazard.msm(msm_model)

c<-sojourn.msm(msm_model)

names(c$estimates)<-c("Tier 1", "Tier 2","Tier 3","Tier 4","Tier 5",
            "Tier 6","Tier 7","Tier 8","Tier 9","Tier 10")

b<-data %>% group_by(Cabel_TV_Band) %>% summarise(Sojourn_months=n())

b$Cabel_TV_Band<-1:10

names(b)[1]<-"Tier"

zero_count<-data %>% group_by(row) %>% filter(Cabel_TV_Band==11) %>% summarise(n())

nrow(zero_count[which(zero_count[,2]==6),])/nrow(zero_count)






 