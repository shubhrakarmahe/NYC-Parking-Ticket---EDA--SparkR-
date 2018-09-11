# -------------------------------------------------------------------------------------
# ------------------------NYC Parking Ticket EDA Case study ---------------------------
# Group Members
# Avishek Sengupta
# Harkirat Dhillon
# Shubhra Karmahe
# Taranveer Singh Arora
#-------------------------------------------------------------------------------------

# Load sparkr library and session
spark_path <- '/usr/local/spark'
if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = spark_path)
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "yarn-client", sparkConfig = list(spark.driver.memory = "1g"))

# load required libraries
library(dplyr)
library(stringr)
library(ggplot2)

# Read data files
df_2015 <- read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2015.csv", 
                   source = "csv", 
                   inferSchema = "true", 
                   header = "true")

df_2016 <- read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2016.csv", 
                   source = "csv", 
                   inferSchema = "true", 
                   header = "true")

df_2017 <- read.df("/common_folder/nyc_parking/Parking_Violations_Issued_-_Fiscal_Year_2017.csv", 
                   source = "csv", 
                   inferSchema = "true", 
                   header = "true")

# Preliminary analysis
dim(df_2015) #11809233 * 51
dim(df_2016) #10626899 * 51
dim(df_2017) #10803028 * 43

printSchema(df_2015)
printSchema(df_2016)
printSchema(df_2017)

str(df_2015)
str(df_2016)
str(df_2017)

head(df_2015)
head(df_2016)
head(df_2017)

# 08 columns - Latitude,Longitude,Community Board,Community Council  , 
#               Census Tract, BIN,BBL and NTA are missing from df_2017

# Rename column names to rmove leadin/trailing and in-between white_spaces
colnames(df_2015)<- str_trim(colnames(df_2015), side= "both")
colnames(df_2015)<- str_replace_all(colnames(df_2015), pattern=" ", replacement = "_")

colnames(df_2016)<- str_trim(colnames(df_2016), side= "both")
colnames(df_2016)<- str_replace_all(colnames(df_2016), pattern=" ", replacement = "_")

colnames(df_2017)<- str_trim(colnames(df_2017), side= "both")
colnames(df_2017)<- str_replace_all(colnames(df_2017), pattern=" ", replacement = "_")

#-----------------------------Data Cleaning ------------------------------

# Summons_number and Issue_Date are 02 key columns. Let's analyse them and perform data cleaning if required.

# Drop Duplicate rows based on Summons_Number if any
df_2015_unique <- dropDuplicates(df_2015,"Summons_Number")
dim(df_2015_unique) # 10951256 * 51

df_2016_unique <- dropDuplicates(df_2016,"Summons_Number")
dim(df_2016_unique) # 10626899 * 51

df_2017_unique <- dropDuplicates(df_2017,"Summons_Number")
dim(df_2017_unique) # 10803028 * 43

# Issue Date - convert it uniform date format 
df_2015_unique$Issue_Date <- SparkR::to_date(df_2015_unique$Issue_Date,
                                               'MM/dd/yyyy')
df_2016_unique$Issue_Date <- SparkR::to_date(df_2016_unique$Issue_Date,
                                               'MM/dd/yyyy')
df_2017_unique$Issue_Date <- SparkR::to_date(df_2017_unique$Issue_Date,
                                               'MM/dd/yyyy')

# Violation_Time is used for one of the aggregation analysis.
# Extract Violation Hour,Minute and Part of Day.

df_2015_unique$Violation_Hour <- SparkR::substr(df_2015_unique$Violation_Time, 1, 2)
df_2015_unique$Violation_Minute <- SparkR::substr(df_2015_unique$Violation_Time, 3, 4)
df_2015_unique$Violation_part_of_day <- SparkR::substr(df_2015_unique$Violation_Time, 5, 5)

df_2016_unique$Violation_Hour <- SparkR::substr(df_2016_unique$Violation_Time, 1, 2)
df_2016_unique$Violation_Minute <- SparkR::substr(df_2016_unique$Violation_Time, 3, 4)
df_2016_unique$Violation_part_of_day <- SparkR::substr(df_2016_unique$Violation_Time, 5, 5)

df_2017_unique$Violation_Hour <- SparkR::substr(df_2017_unique$Violation_Time, 1, 2)
df_2017_unique$Violation_Minute <- SparkR::substr(df_2017_unique$Violation_Time, 3, 4)
df_2017_unique$Violation_part_of_day <- SparkR::substr(df_2017_unique$Violation_Time, 5, 5)

# Create temp view to proceed further with analysis
createOrReplaceTempView(df_2015_unique, 
                        "df_view_2015")
createOrReplaceTempView(df_2016_unique, 
                        "df_view_2016")
createOrReplaceTempView(df_2017_unique, 
                        "df_view_2017")

#------------------------Fiscal Year Range---------------------------------------------------
# max 2015-06-30 & min 1985-07-16
issue_date_2015_1 <- SparkR::sql("select max(`Issue_Date`),
                                 min(`Issue_Date`) 
                                 from df_view_2015")
head(issue_date_2015_1)

issue_date_2015_2 <- SparkR::sql("select year(`Issue_Date`),
                                 count(`Issue_Date`) 
                                 from df_view_2015
                                 group by year(`Issue_Date`)
                                 order by 2 desc")
head(issue_date_2015_2, 20)

# max 2069-10-02 & min 1970-04-13
issue_date_2016_1 <- SparkR::sql("select max(`Issue_Date`),
                                 min(`Issue_Date`) 
                                 from df_view_2016")
head(issue_date_2016_1)

issue_date_2016_2 <- SparkR::sql("select year(`Issue_Date`),
                                 count(`Issue_Date`) 
                                 from df_view_2016
                                 group by year(`Issue_Date`)
                                 order by 2 desc")
head(issue_date_2016_2, 20)

# max - 2069-11-19 & min 1972-03-30
issue_date_2017_1 <- SparkR::sql("select max(`Issue_Date`),
                                 min(`Issue_Date`) 
                                 from df_view_2017")
head(issue_date_2017_1)

issue_date_2017_2 <- SparkR::sql("select year(`Issue_Date`),
                                 count(`Issue_Date`) 
                                 from df_view_2017
                                 group by year(`Issue_Date`)
                                 order by 2 desc")
head(issue_date_2017_2, 20)

# fiscal year for Newyork (April to March)  https://en.wikipedia.org/wiki/Fiscal_year
# df_view_2015 - issue date to be considered for analysis (Apr 2014 -Mar 2015) 
# df_view_2016 - issue date to be considered for analysis (Apr 2015 -Mar 2016)
# df_view_2017 - issue date to be considered for analysis (Apr 2016 -Mar 2017)

issue_date_2015_3 <- SparkR::sql("select concat(year(`Issue_Date`),'/',month(`Issue_Date`)) ,
                                count(`Issue_Date`)
                                 from df_view_2015
                                 where `Issue_Date` between '2014-04-01' and '2015-03-31'
                                 group by concat(year(`Issue_Date`),'/',month(`Issue_Date`))
                                 order by 1 desc")
head(issue_date_2015_3, 12)

issue_date_2016_3 <- SparkR::sql("select concat(year(`Issue_Date`),'/',month(`Issue_Date`)) ,
                                count(`Issue_Date`)
                                 from df_view_2016
                                 where `Issue_Date` between '2015-04-01' and '2016-03-31'
                                 group by concat(year(`Issue_Date`),'/',month(`Issue_Date`))
                                 order by 1 desc")
head(issue_date_2016_3, 12)

issue_date_2017_3 <- SparkR::sql("select concat(year(`Issue_Date`),'/',month(`Issue_Date`)) ,
                                count(`Issue_Date`)
                                 from df_view_2017
                                 where where `Issue_Date` between '2016-04-01' and '2017-03-31'
                                 group by concat(year(`Issue_Date`),'/',month(`Issue_Date`))
                                 order by 1 desc")
head(issue_date_2017_3, 12)

# ----------------------Subset data as per fiscal year range-------------------------------------------------

df_view_2015 <- SparkR::sql("select * from df_view_2015
                                 where where `Issue_Date` between '2014-04-01' and '2015-03-31'")
dim(df_view_2015) # 8033544 * 54

df_view_2016 <- SparkR::sql("select * from df_view_2016
                                 where where `Issue_Date` between '2015-04-01' and '2016-03-31'")
dim(df_view_2016) # 8417338 * 54

df_view_2017 <- SparkR::sql("select * from df_view_2017
                                 where where `Issue_Date` between '2016-04-01' and '2017-03-31'")
dim(df_view_2017) # 8035882 * 46

#------------------------------Drop Columns --------------------------------------------------
# Analyse columns having missing values and drop if they are all NULL

missing_2015 <- SparkR::sql("SELECT COUNT(*) total_null_cnt,
                                  SUM(CASE WHEN `No_Standing_or_Stopping_Violation` IS NULL THEN 1 ELSE 0 END) as no_standing_or_stopping_violation,
                                  SUM(CASE WHEN `Latitude` IS NULL THEN 1 ELSE 0 END) as latitude,
                                  SUM(CASE WHEN `Longitude` IS NULL THEN 1 ELSE 0 END) as longitude,
                                  SUM(CASE WHEN `Hydrant_Violation` IS NULL THEN 1 ELSE 0 END) as hydrant_violation,
                                  SUM(CASE WHEN `Double_Parking_Violation` IS NULL THEN 1 ELSE 0 END) as double_parking_violation,
                                  SUM(CASE WHEN `Community_Board` IS NULL THEN 1 ELSE 0 END) as community_board,
                                  SUM(CASE WHEN `Community_Council` IS NULL THEN 1 ELSE 0 END) as community_council,
                                  SUM(CASE WHEN `Census_Tract` IS NULL THEN 1 ELSE 0 END) as census_tract,
                                  SUM(CASE WHEN `BBL` IS NULL THEN 1 ELSE 0 END) as BBL,
                                  SUM(CASE WHEN `BIN` IS NULL THEN 1 ELSE 0 END) as BIN,
                                  SUM(CASE WHEN `NTA` IS NULL THEN 1 ELSE 0 END) as NTA
                          FROM df_view_2015")
head(missing_2015)

missing_2016 <- SparkR::sql("SELECT COUNT(*) total_null_cnt,
                                  SUM(CASE WHEN `No_Standing_or_Stopping_Violation` IS NULL THEN 1 ELSE 0 END) as no_standing_or_stopping_violation,
                                  SUM(CASE WHEN `Latitude` IS NULL THEN 1 ELSE 0 END) as latitude,
                                  SUM(CASE WHEN `Longitude` IS NULL THEN 1 ELSE 0 END) as longitude,
                                  SUM(CASE WHEN `Hydrant_Violation` IS NULL THEN 1 ELSE 0 END) as hydrant_violation,
                                  SUM(CASE WHEN `Double_Parking_Violation` IS NULL THEN 1 ELSE 0 END) as double_parking_violation,
                                  SUM(CASE WHEN `Community_Board` IS NULL THEN 1 ELSE 0 END) as community_board,
                                  SUM(CASE WHEN `Community_Council` IS NULL THEN 1 ELSE 0 END) as community_council,
                                  SUM(CASE WHEN `Census_Tract` IS NULL THEN 1 ELSE 0 END) as census_tract,
                                  SUM(CASE WHEN `BBL` IS NULL THEN 1 ELSE 0 END) as BBL,
                                  SUM(CASE WHEN `BIN` IS NULL THEN 1 ELSE 0 END) as BIN,
                                  SUM(CASE WHEN `NTA` IS NULL THEN 1 ELSE 0 END) as NTA
                          FROM df_view_2016")
head(missing_2016)

missing_2017 <- SparkR::sql("SELECT COUNT(*) total_null_cnt,
                                   SUM(CASE WHEN `No_Standing_or_Stopping_Violation` IS NULL THEN 1 ELSE 0 END) as no_standing_or_stopping_violation,
                                  SUM(CASE WHEN `Hydrant_Violation` IS NULL THEN 1 ELSE 0 END) as hydrant_violation,
                                  SUM(CASE WHEN `Double_Parking_Violation` IS NULL THEN 1 ELSE 0 END) as double_parking_violation
                                   FROM df_view_2017")
head(missing_2017)

df_view_2015 <- drop(df_view_2015,c("No_Standing_or_Stopping_Violation","Latitude","Longitude",
                                    "Hydrant_Violation","Double_Parking_Violation","Community_Board",
                                    "Community_Council","Census_Tract","BBL","BIN","NTA"))
dim(df_view_2015) #8033544 * 43

df_view_2016 <- drop(df_view_2016,c("No_Standing_or_Stopping_Violation","Latitude","Longitude",
                                    "Hydrant_Violation","Double_Parking_Violation","Community_Board",
                                    "Community_Council","Census_Tract","BBL","BIN","NTA"))
dim(df_view_2016) #8417338 * 43

df_view_2017 <- drop(df_view_2017,c("No_Standing_or_Stopping_Violation","Hydrant_Violation",
                                    "Double_Parking_Violation"))
dim(df_view_2017) #8035882 * 43

# ----------------------Examine Data -------------------------------------------------

# 1. Find total number of tickets for each year.

# --------------------Analysis Obesrvation ----------------------------------------
# 10951256 tickets for year 2015
# 10626899 tickets for year 2016
# 10803028 tickets for year 2017

no_of_tickets_2015 <- SparkR::sql("SELECT COUNT(Summons_Number) as total_tickets
                                  FROM df_view_2015")
head(no_of_tickets_2015)

no_of_tickets_2016 <- SparkR::sql("SELECT COUNT(Summons_Number) as total_tickets
                                  FROM df_view_2016")
head(no_of_tickets_2016)

no_of_tickets_2017 <- SparkR::sql("SELECT COUNT(Summons_Number) as total_tickets
                                  FROM df_view_2017")
head(no_of_tickets_2017)

# Visual Analysis -
no_of_tickets_overall <- data.frame(rbind(head(no_of_tickets_2015),
                                          head(no_of_tickets_2016),
                                          head(no_of_tickets_2017)))
no_of_tickets_overall <- cbind(data.frame(rbind("FY-2015","FY-2016","FY-2017")), 
                               no_of_tickets_overall)
colnames(no_of_tickets_overall) <- c("FY-Year","Total Tickets")

head(no_of_tickets_overall)

ggplot(data = no_of_tickets_overall,aes(x = `FY-Year`,y = `Total Tickets` )) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) +
  geom_text(aes(label = `Total Tickets`),vjust = 0.05)
#------------------------------------------------------------------------------------
# 2. Find out how many unique states the cars which got parking tickets came from.

# ------------------------Analysis Observation ----------------
# 69 unique states for year 2015
# 68 unique states for year 2016
# 67 unique states for year 2017

state_2015 <- SparkR::sql("SELECT Registration_State,
                                  count(Registration_State)
                                  FROM df_view_2015
                                  group by Registration_State
                                  order by 2 desc")
head(state_2015,nrow(state_2015))

state_2016 <- SparkR::sql("SELECT Registration_State,
                                  count(Registration_State)
                                  FROM df_view_2016
                                  group by Registration_State
                                  order by 2 desc")
head(state_2016,nrow(state_2016))

state_2017 <- SparkR::sql("SELECT Registration_State,
                                  count(Registration_State)
                                  FROM df_view_2017
                                  group by Registration_State
                                  order by 2 desc")
head(state_2017,nrow(state_2017))

# Visual Analysis -
no_of_states_overall <- data.frame(rbind(nrow(state_2015),
                                         nrow(state_2016),
                                         nrow(state_2017)))
no_of_states_overall <- cbind(data.frame(rbind("FY-2015","FY-2016","FY-2017")), 
                              no_of_states_overall)
colnames(no_of_states_overall) <- c("FY-Year","Total States")

head(no_of_states_overall)

ggplot(data = no_of_states_overall,aes(x = `FY-Year`,y = `Total States` )) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) +
  geom_text(aes(label = `Total States`),vjust = 0.05)
#------------------------------------------------------------------------------------

# 3. Some parking tickets don’t have addresses on them, which is cause for concern. 
 # Find out how many such tickets there are.

# ------------------------Analysis Observation ----------------
# 16.5% missing address for 2015 
# 19% missing address for 2016
# 21% missing address for 2017

missing_address_2015 <- SparkR::sql("SELECT (SUM(CASE WHEN (House_Number IS NULL or Street_Name IS NULL) 
                                    THEN 1 ELSE 0 END)*100)/count(*) as missing_address_percentage
                                  FROM df_view_2015")
head(missing_address_2015)

missing_address_2016 <- SparkR::sql("SELECT (SUM(CASE WHEN (House_Number IS NULL or Street_Name IS NULL) 
                                    THEN 1 ELSE 0 END)*100)/count(*) as missing_address_percentage
                                  FROM df_view_2016")
head(missing_address_2016)

missing_address_2017 <- SparkR::sql("SELECT (SUM(CASE WHEN (House_Number IS NULL or Street_Name IS NULL) 
                                    THEN 1 ELSE 0 END)*100)/count(*) as missing_address_percentage
                                  FROM df_view_2017")
head(missing_address_2017)

# Visual Analysis -
miss_addr_overall <- data.frame(rbind(head(missing_address_2015),
                                      head(missing_address_2016),
                                      head(missing_address_2017)))
miss_addr_overall <- cbind(data.frame(rbind("FY-2015","FY-2016","FY-2017")), 
                           miss_addr_overall)
colnames(miss_addr_overall) <- c("FY-Year","Percentage of Missing addresses")

head(miss_addr_overall)

ggplot(data = miss_addr_overall,aes(x = `FY-Year`,y = round(`Percentage of Missing addresses`))) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + ylab("% of Missing Addresses") +
  geom_text(aes(label = round(`Percentage of Missing addresses`)),vjust = 0.05)

# ------------------Aggregation Task ----------------------------------------------

# 1. How often does each violation code occur? (frequency of violation codes - find the top 5)

# ------------------------Analysis Obervation ----------------
# Violation Code - year 2015 - 21 - 1501614,38 - 1324586,14 - 924627,36 - 761571,37 - 746278
# Violation Code - year 2016 - 21 - 1531587,36 - 1253512,38 - 1143696,14 - 875614,37 - 686610
# Violation Code - year 2017 - 21 - 1528588,36 - 1400614,38 - 1062304,14 - 893498,20 - 618593

freq_violation_2015 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2015
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_2015,5)

freq_violation_2016 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2016
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_2016,5)

freq_violation_2017 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2017
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_2017,5)

#Visual analysis
freq_violation_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY-2015",5),head(freq_violation_2015,5))),
                                           (cbind("FY_Year" = rep("FY-2016",5),head(freq_violation_2016,5))),
                                           (cbind("FY_Year" = rep("FY-2017",5),head(freq_violation_2017,5)))))

head(freq_violation_overall,15)

ggplot(data = freq_violation_overall,aes(x = factor(Violation_Code),y = freq)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Violation Code") + ylab("Frequency")+
  facet_grid('FY_Year')
# -------------------------------------------------------------------------------
# 2. How often does each vehicle body type get a parking ticket? 
# How about the vehicle make? (find the top 5 for both)

# ------------------------Analysis Obervation ----------------
# Vehicle body type - year 2015 - SUBN - 3451963,4DSD - 3102510,VAN - 1605228,DELV - 840441 and SDN - 453992
# Vehicle body type - year 2016 - SUBN - 3466037,4DSD - 2992107,VAN - 1518303,DELV - 755282 and SDN - 424043
# Vehicle body type - year 2017 - SUBN - 3719802,4DSD - 3082020,VAN - 1411970,DELV - 687330 and SDN - 438191

# Vehicle make - year 2015 - FORD - 1417303,TOYOT - 1123523,HONDA - 1018049,NISSA - 837569 and CHEVR - 836389
# Vehicle make - year 2016 - FORD- 1324774,TOYOT - 1154790,HONDA - 1014074,NISSA - 834833 and CHEVR- 759663
# Vehicle make - year 2017 - FORD -1280958,TOYOT - 1211451,HONDA - 1079238,NISSA - 918590 and CHEVR - 714655

vech_body_2015 <- SparkR::sql("SELECT Vehicle_Body_Type, count(Vehicle_Body_Type) as freq
                                  FROM df_view_2015
                                   group by Vehicle_Body_Type
                                   order by 2 desc")
head(vech_body_2015,5)

vech_body_2016 <- SparkR::sql("SELECT Vehicle_Body_Type, count(Vehicle_Body_Type) as freq
                                  FROM df_view_2016
                                   group by Vehicle_Body_Type
                                   order by 2 desc")
head(vech_body_2016,5)

vech_body_2017 <- SparkR::sql("SELECT Vehicle_Body_Type, count(Vehicle_Body_Type) as freq
                                  FROM df_view_2017
                                   group by Vehicle_Body_Type
                                   order by 2 desc")
head(vech_body_2017,5)

# Visual Analysis

vech_body_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY-2015",5),head(vech_body_2015,5))),
                                      (cbind("FY_Year" = rep("FY-2016",5),head(vech_body_2016,5))),
                                      (cbind("FY_Year" = rep("FY-2017",5),head(vech_body_2017,5)))))

head(vech_body_overall,15)

ggplot(data = vech_body_overall,aes(x = factor(Vehicle_Body_Type),y = freq)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Vehicle Body Type") + ylab("Frequency")+
  facet_grid('FY_Year')

vech_make_2015 <- SparkR::sql("SELECT Vehicle_Make, count(Vehicle_Make) as freq
                                  FROM df_view_2015
                                   group by Vehicle_Make
                                   order by 2 desc")
head(vech_make_2015,5)

vech_make_2016 <- SparkR::sql("SELECT Vehicle_Make, count(Vehicle_Make) as freq
                                  FROM df_view_2016
                                   group by Vehicle_Make
                                   order by 2 desc")
head(vech_make_2016,5)

vech_make_2017 <- SparkR::sql("SELECT Vehicle_Make, count(Vehicle_Make) as freq
                                  FROM df_view_2017
                                   group by Vehicle_Make
                                   order by 2 desc")
head(vech_make_2017,5)
 
#visual analysis
vech_make_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY-2015",5),head(vech_make_2015,5))),
                                      (cbind("FY_Year" = rep("FY-2016",5),head(vech_make_2016,5))),
                                      (cbind("FY_Year" = rep("FY-2017",5),head(vech_make_2017,5)))))

head(vech_make_overall,15)

ggplot(data = vech_make_overall,aes(x = factor(Vehicle_Make),y = freq)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Vehicle Make Type") + ylab("Frequency")+
  facet_grid('FY_Year')
#---------------------------------------------------------------------------

# 3. A precinct is a police station that has a certain zone of the city under its command. 
# Find the (5 highest) frequencies of:
# 3.1 Violating Precincts (this is the precinct of the zone where the violation occurred). 
# Using this, can you make any insights for parking violations in any specific areas of the city? 
# 3.2 Issuing Precincts (this is the precinct that issued the ticket)

# ------------------------Analysis Obervation ----------------

# Violation Precinct for year 2015 - 19 - 559716,18 - 400887,14 -384596,1 - 307808,114 - 300557
# Violation Precinct for year 2016 - 19 - 554465,18 - 331704,14 - 324467,1 - 303850,114 - 291336
# Violation Precinct for year 2017 - 19 - 535671,14 - 352450,1 - 331810,18 - 306920,114 - 296514

# Issuer Precinct for year 2015 - 19 - 544946,18 - 391501,14 - 369725,1 - 298594,114 - 295601
# Issuer Precinct for year 2016 - 19 - 540569,18 - 323132,14 - 315311,1 - 295013, 114 - 286924
# Issuer Precinct for year 2017 - 19 - 521513,14 - 344977,1 - 321170,18- 296553,114 - 289950


violation_precinct_2015 <- SparkR::sql("SELECT Violation_Precinct, count(Violation_Precinct) as freq
                                  FROM df_view_2015
                                  where Violation_Precinct <> 0
                                   group by Violation_Precinct
                                    order by 2 desc")
head(violation_precinct_2015,5)

violation_precinct_2016 <- SparkR::sql("SELECT Violation_Precinct, count(Violation_Precinct) as freq
                                  FROM df_view_2016
                                  where Violation_Precinct <> 0
                                   group by Violation_Precinct
                                   order by 2 desc")
head(violation_precinct_2016,5)


violation_precinct_2017 <- SparkR::sql("SELECT Violation_Precinct, count(Violation_Precinct) as freq
                                  FROM df_view_2017
                                  where Violation_Precinct <> 0
                                   group by Violation_Precinct
                                   order by 2 desc")
head(violation_precinct_2017,5)

# Visual Analysis
violation_precinct_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY-2015",5),head(violation_precinct_2015,5))),
                                               (cbind("FY_Year" = rep("FY-2016",5),head(violation_precinct_2016,5))),
                                               (cbind("FY_Year" = rep("FY-2017",5),head(violation_precinct_2017,5)))))

head(violation_precinct_overall,15)

ggplot(data = violation_precinct_overall,aes(x = factor(Violation_Precinct),y = freq)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Violation Precinct") + ylab("Frequency")+
  facet_grid('FY_Year')

issuer_precinct_2015 <- SparkR::sql("SELECT Issuer_Precinct, count(Issuer_Precinct) as freq
                                  FROM df_view_2015
                                  where Issuer_Precinct <> 0
                                   group by Issuer_Precinct
                                   order by 2 desc")
head(issuer_precinct_2015,5)

issuer_precinct_2016 <- SparkR::sql("SELECT Issuer_Precinct, count(Issuer_Precinct) as freq
                                  FROM df_view_2016
                                  where Issuer_Precinct <> 0
                                   group by Issuer_Precinct
                                   order by 2 desc")
head(issuer_precinct_2016,5)

issuer_precinct_2017 <- SparkR::sql("SELECT Issuer_Precinct, count(Issuer_Precinct) as freq
                                  FROM df_view_2017
                                  where Issuer_Precinct <> 0
                                   group by Issuer_Precinct
                                   order by 2 desc")
head(issuer_precinct_2017,5)


#visual analysis
issuer_precinct_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY-2015",5),head(issuer_precinct_2015,5))),
                                               (cbind("FY_Year" = rep("FY-2016",5),head(issuer_precinct_2016,5))),
                                               (cbind("FY_Year" = rep("FY-2017",5),head(issuer_precinct_2017,5)))))

head(issuer_precinct_overall,15)

ggplot(data = issuer_precinct_overall,aes(x = factor(Issuer_Precinct),y = freq)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Issuer Precinct") + ylab("Frequency")+
  facet_grid('FY_Year')

# -----------------------------------------------------------------------------
# 4. Find the violation code frequency across 3 precincts which have issued the most number of tickets 
# - do these precinct zones have an exceptionally high frequency of certain violation codes? 
# Are these codes common across precincts?

# ----------------------Analysis Observation ------------------------------
# Violation code for Issuer percinct 19 - year 2015 - 38 - 90437,37 - 79738,14 - 60589,21 - 56416,16 - 56318
# Violation code for Issuer percinct 18 - year 2015 - 14 - 121004,69 - 57218,31 - 30447,47 - 29124,42 - 19820
# Violation code for Issuer percinct 14 - year 2015 - 69 - 80368,14 - 77269,31 - 41049,42 - 28114,47 - 27229

# Violation code for Issuer percinct 19 - year 2016 - 38 - 77183,37 - 75641,46 - 73016,14 - 61742,21 - 58719
# Violation code for Issuer percinct 18 - year 2016 - 14 - 99857,69 - 47881,47 - 24009,31 - 22809,42 - 17678
# Violation code for Issuer percinct 14 - year 2016 - 69 - 67932,14 - 62426,31 - 35711,47- 24450,42 - 23662

# Violation code for Issuer percinct 19 - year 2017 - 46 - 86390,37 - 72437,38 - 72344,14 - 57563,21 - 54700
# Violation code for Issuer percinct 14 - year 2017 - 14 - 73837,69 -58026,31 - 39857,47 - 30540,42 - 20663
# Violation code for Issuer percinct 1 - year 2017 -14 - 73522,16 - 38937,20 - 27841,46 - 22534,38 - 16989


freq_violation_percinct_19_2015 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2015
                                  WHERE Issuer_Precinct = 19
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_percinct_19_2015,5)

freq_violation_percinct_18_2015 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2015
                                  WHERE Issuer_Precinct = 18
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_percinct_18_2015,5)

freq_violation_percinct_14_2015 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2015
                                  WHERE Issuer_Precinct = 14
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_percinct_14_2015,5)

# Visual Analysis
violation_precinct_2015_overall <- data.frame(rbind((cbind("Violation_Precinct" = rep("19",5),head(freq_violation_percinct_19_2015,5))),
                                                    (cbind("Violation_Precinct" = rep("18",5),head(freq_violation_percinct_18_2015,5))),
                                                    (cbind("Violation_Precinct" = rep("14",5),head(freq_violation_percinct_14_2015,5)))))

head(violation_precinct_2015_overall,15)

ggplot(data = violation_precinct_2015_overall,aes(x = factor(Violation_Code),y = freq)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Violation Code") + ylab("Frequency")+
  facet_grid('Violation_Precinct') +ggtitle("FY - 2015")


freq_violation_percinct_19_2016 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2016
                                  WHERE Issuer_Precinct = 19
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_percinct_19_2016,5)

freq_violation_percinct_18_2016 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2016
                                  WHERE Issuer_Precinct = 18
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_percinct_18_2016,5)

freq_violation_percinct_14_2016 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2016
                                  WHERE Issuer_Precinct = 14
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_percinct_14_2016,5)

#visual analysis
violation_precinct_2016_overall <- data.frame(rbind((cbind("Violation_Precinct" = rep("19",5),head(freq_violation_percinct_19_2016,5))),
                                                    (cbind("Violation_Precinct" = rep("18",5),head(freq_violation_percinct_18_2016,5))),
                                                    (cbind("Violation_Precinct" = rep("14",5),head(freq_violation_percinct_14_2016,5)))))

head(violation_precinct_2016_overall,15)

ggplot(data = violation_precinct_2016_overall,aes(x = factor(Violation_Code),y = freq)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Violation Code") + ylab("Frequency")+
  facet_grid('Violation_Precinct') +ggtitle("FY - 2016")


freq_violation_percinct_19_2017 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2017
                                  WHERE Issuer_Precinct = 19
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_percinct_19_2017,5)

freq_violation_percinct_14_2017 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2017
                                  WHERE Issuer_Precinct = 14
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_percinct_14_2017,5)

freq_violation_percinct_1_2017 <- SparkR::sql("SELECT Violation_Code, count(Violation_Code) as freq
                                  FROM df_view_2017
                                  WHERE Issuer_Precinct = 1
                                   group by Violation_Code
                                   order by 2 desc")
head(freq_violation_percinct_1_2017,5)


#visual analysis
violation_precinct_2017_overall <- data.frame(rbind((cbind("Violation_Precinct" = rep("19",5),head(freq_violation_percinct_19_2017,5))),
                                                    (cbind("Violation_Precinct" = rep("14",5),head(freq_violation_percinct_14_2017,5))),
                                                    (cbind("Violation_Precinct" = rep("1",5),head(freq_violation_percinct_1_2017,5)))))

head(violation_precinct_2017_overall,15)

ggplot(data = violation_precinct_2017_overall,aes(x = factor(Violation_Code),y = freq)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Violation Code") + ylab("Frequency")+
  facet_grid('Violation_Precinct') +ggtitle("FY - 2017")
#-------------------------------------------------------------------------------------------------
# 5. You’d want to find out the properties of parking violations across different times of the day:
# The Violation Time field is specified in a strange format. Find a way to make this into a time attribute 
# that you can use to divide into groups.

# Find a way to deal with missing values, if any.

# Violation_time  - Missing vaue analysis 
# Missing value % is too small for 2016,2016 & 2017. We will ignore them during our analysis.
missing_violation_time_2015 <- SparkR::sql("select (SUM(CASE WHEN `Violation_Time` IS NULL THEN 1 ELSE 0 END)/count(*))*100 as Missing_Violation_Time_percentage
                                            from df_view_2015")

head(missing_violation_time_2015)

missing_violation_time_2016 <- SparkR::sql("select (SUM(CASE WHEN `Violation_Time` IS NULL THEN 1 ELSE 0 END)/count(*))*100 as Missing_Violation_Time_percentage
                                            from df_view_2016")

head(missing_violation_time_2016)

missing_violation_time_2017 <- SparkR::sql("select (SUM(CASE WHEN `Violation_Time` IS NULL THEN 1 ELSE 0 END)/count(*))*100 as Missing_Violation_Time_percentage
                                            from df_view_2017")

head(missing_violation_time_2017)

# Divide 24 hours into 6 equal discrete bins of time. The intervals you choose are at your discretion. 
# For each of these groups, find the 3 most commonly occurring violations

violation_time_bin_2015 <- SparkR::sql("select Violation_Hour,Violation_Time, Violation_part_of_day,Violation_Code,
                                        CASE WHEN (((Violation_Hour BETWEEN 0 AND 3) OR (Violation_Hour = 12))AND Violation_part_of_day = 'A') THEN '0-3'
                                        WHEN ((Violation_Hour BETWEEN 4 AND 7) AND Violation_part_of_day = 'A') THEN '4-7'
                                        WHEN ((Violation_Hour BETWEEN 8 AND 11) AND Violation_part_of_day = 'A') THEN '8-11'
                                        WHEN (((Violation_Hour BETWEEN 0 AND 3) OR (Violation_Hour = 12))AND Violation_part_of_day = 'P') THEN '12-15'
                                        WHEN ((Violation_Hour BETWEEN 4 AND 7) AND Violation_part_of_day = 'P') THEN '16-19'
                                        WHEN ((Violation_Hour BETWEEN 8 AND 11) AND Violation_part_of_day = 'P') THEN '20-23'
                                        END as Violation_Time_bin
                                        from df_view_2015
                                        where Violation_Time is not null")

head(violation_time_bin_2015,20)

createOrReplaceTempView(violation_time_bin_2015, 
                        "df_view_violation_time_bin_2015")

violation_time_bin_code_2015 <- SparkR::sql("select Violation_Time_bin,Violation_Code,freq
                                        from (select Violation_Time_bin,Violation_Code,freq,
                                        dense_rank() over(partition by Violation_Time_bin order by freq desc) as d_rank
                                        from (select Violation_Time_bin,Violation_Code,count(*) as freq
                                        from df_view_violation_time_bin_2015
                                        where Violation_Time_bin is not null
                                        group by Violation_Time_bin,Violation_Code))
                                        where d_rank <= 3")
head(violation_time_bin_code_2015,nrow(violation_time_bin_code_2015))

violation_time_bin_2016 <- SparkR::sql("select Violation_Hour,Violation_Time, Violation_part_of_day,Violation_Code,
                                        CASE WHEN (((Violation_Hour BETWEEN 0 AND 3) OR (Violation_Hour = 12))AND Violation_part_of_day = 'A') THEN '0-3'
                                       WHEN ((Violation_Hour BETWEEN 4 AND 7) AND Violation_part_of_day = 'A') THEN '4-7'
                                       WHEN ((Violation_Hour BETWEEN 8 AND 11) AND Violation_part_of_day = 'A') THEN '8-11'
                                       WHEN (((Violation_Hour BETWEEN 0 AND 3) OR (Violation_Hour = 12))AND Violation_part_of_day = 'P') THEN '12-15'
                                       WHEN ((Violation_Hour BETWEEN 4 AND 7) AND Violation_part_of_day = 'P') THEN '16-19'
                                       WHEN ((Violation_Hour BETWEEN 8 AND 11) AND Violation_part_of_day = 'P') THEN '20-23'
                                       END as Violation_Time_bin
                                       from df_view_2016
                                       where Violation_Time is not null")

head(violation_time_bin_2016,20)

createOrReplaceTempView(violation_time_bin_2016, 
                        "df_view_violation_time_bin_2016")

violation_time_bin_code_2016 <- SparkR::sql("select Violation_Time_bin,Violation_Code,freq
                                            from (select Violation_Time_bin,Violation_Code,freq,
                                            dense_rank() over(partition by Violation_Time_bin order by freq desc) as d_rank
                                            from (select Violation_Time_bin,Violation_Code,count(*) as freq
                                            from df_view_violation_time_bin_2016
                                            where Violation_Time_bin is not null
                                            group by Violation_Time_bin,Violation_Code))
                                            where d_rank <= 3")
head(violation_time_bin_code_2016,nrow(violation_time_bin_code_2016))

violation_time_bin_2017 <- SparkR::sql("select Violation_Hour,Violation_Time, Violation_part_of_day,Violation_Code,
                                        CASE WHEN (((Violation_Hour BETWEEN 0 AND 3) OR (Violation_Hour = 12))AND Violation_part_of_day = 'A') THEN '0-3'
                                       WHEN ((Violation_Hour BETWEEN 4 AND 7) AND Violation_part_of_day = 'A') THEN '4-7'
                                       WHEN ((Violation_Hour BETWEEN 8 AND 11) AND Violation_part_of_day = 'A') THEN '8-11'
                                       WHEN (((Violation_Hour BETWEEN 0 AND 3) OR (Violation_Hour = 12))AND Violation_part_of_day = 'P') THEN '12-15'
                                       WHEN ((Violation_Hour BETWEEN 4 AND 7) AND Violation_part_of_day = 'P') THEN '16-19'
                                       WHEN ((Violation_Hour BETWEEN 8 AND 11) AND Violation_part_of_day = 'P') THEN '20-23'
                                       END as Violation_Time_bin
                                       from df_view_2017
                                       where Violation_Time is not null")

head(violation_time_bin_2017,20)

createOrReplaceTempView(violation_time_bin_2017, 
                        "df_view_violation_time_bin_2017")

violation_time_bin_code_2017 <- SparkR::sql("select Violation_Time_bin,Violation_Code,freq
                                            from (select Violation_Time_bin,Violation_Code,freq,
                                            dense_rank() over(partition by Violation_Time_bin order by freq desc) as d_rank
                                            from (select Violation_Time_bin,Violation_Code,count(*) as freq
                                            from df_view_violation_time_bin_2017
                                            where Violation_Time_bin is not null
                                            group by Violation_Time_bin,Violation_Code))
                                            where d_rank <= 3")
head(violation_time_bin_code_2017,nrow(violation_time_bin_code_2017))

# visual analysis
violation_time_bin_code_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY_2015",nrow(violation_time_bin_code_2015)),head(violation_time_bin_code_2015,nrow(violation_time_bin_code_2015)))),
                                                    (cbind("FY_Year" = rep("FY_2016",nrow(violation_time_bin_code_2016)),head(violation_time_bin_code_2016,nrow(violation_time_bin_code_2016)))),
                                                    (cbind("FY_Year" = rep("FY_2017",nrow(violation_time_bin_code_2017)),head(violation_time_bin_code_2017,nrow(violation_time_bin_code_2017))))))

head(violation_time_bin_code_overall,nrow(violation_time_bin_code_overall))

ggplot(data = violation_time_bin_code_overall,aes(x = factor(Violation_Code),y = freq,fill=as.factor(Violation_Time_bin))) +
  geom_col(col = "black", width = 0.5) + xlab("Violation Code") + ylab("Frequency")+
  facet_grid('FY_Year')

# Now, try another direction. For the 3 most commonly occurring violation codes, 
# find the most common times of day (in terms of the bins from the previous part)

# Year 2015 - Top 3 Violation code - 21,38,14
violation_code_time_bin_2015 <- SparkR::sql("select Violation_Code,Violation_Time_bin,freq
                                            from (select Violation_Time_bin,Violation_Code,freq,
                                            dense_rank() over(partition by Violation_Code order by freq desc) as d_rank
                                            from
                                            (select Violation_Code,Violation_Time_bin,count(*) as freq
                                            from df_view_violation_time_bin_2015
                                            where Violation_Time_bin is not null
                                            and Violation_Code in (21,38,14)
                                            group by Violation_Code,Violation_Time_bin
                                            order by Violation_Code,Violation_Time_bin,freq desc))
                                            where d_rank == 1")
head(violation_code_time_bin_2015,nrow(violation_code_time_bin_2015))

# Year 2016 - Top 3 Violation code - 21,36,38
violation_code_time_bin_2016 <- SparkR::sql("select Violation_Code,Violation_Time_bin,freq
                                            from (select Violation_Time_bin,Violation_Code,freq,
                                            dense_rank() over(partition by Violation_Code order by freq desc) as d_rank
                                            from
                                            (select Violation_Code,Violation_Time_bin,count(*) as freq
                                            from df_view_violation_time_bin_2016
                                            where Violation_Time_bin is not null
                                            and Violation_Code in (21,36,38)
                                            group by Violation_Code,Violation_Time_bin
                                            order by Violation_Code,Violation_Time_bin,freq desc))
                                            where d_rank == 1")
head(violation_code_time_bin_2016,nrow(violation_code_time_bin_2016))

# Year 2017 - Top 3 Violation code - 21,36,38
violation_code_time_bin_2017 <- SparkR::sql("select Violation_Code,Violation_Time_bin,freq
                                            from (select Violation_Time_bin,Violation_Code,freq,
                                            dense_rank() over(partition by Violation_Code order by freq desc) as d_rank
                                            from
                                            (select Violation_Code,Violation_Time_bin,count(*) as freq
                                            from df_view_violation_time_bin_2017
                                            where Violation_Time_bin is not null
                                            and Violation_Code in (21,36,38)
                                            group by Violation_Code,Violation_Time_bin
                                            order by Violation_Code,Violation_Time_bin,freq desc))
                                            where d_rank == 1")
head(violation_code_time_bin_2017,nrow(violation_code_time_bin_2017))

#visual analysis
violation_code_time_bin_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY_2015",nrow(violation_code_time_bin_2015)),head(violation_code_time_bin_2015,nrow(violation_code_time_bin_2015)))),
                                                    (cbind("FY_Year" = rep("FY_2016",nrow(violation_code_time_bin_2016)),head(violation_code_time_bin_2016,nrow(violation_code_time_bin_2016)))),
                                                    (cbind("FY_Year" = rep("FY_2017",nrow(violation_code_time_bin_2017)),head(violation_code_time_bin_2017,nrow(violation_code_time_bin_2017))))))

head(violation_code_time_bin_overall,nrow(violation_code_time_bin_overall))

ggplot(data = violation_code_time_bin_overall,aes(x = factor(Violation_Time_bin),y = freq,fill=as.factor(Violation_Code))) +
  geom_col(col = "black", width = 0.5) + xlab("Violation Time bin") + ylab("Frequency")+
  facet_grid('FY_Year')

#---------------------------------------------------------------------------------------
# 6. Let’s try and find some seasonality in this data
# First, divide the year into some number of seasons, and find frequencies of tickets for each season.

season_bin_2015 <- SparkR::sql("select 
                                case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end as season_bin, count(Issue_Date) as ticket_freq
                                from df_view_2015
                               group by case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end 
                               order by 2 desc")
head(season_bin_2015,20)

season_bin_2016 <- SparkR::sql("select 
                                case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end as season_bin, count(Issue_Date) as ticket_freq
                                from df_view_2016
                               group by case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end
                               order by 2 desc")
head(season_bin_2016,20)

season_bin_2017 <- SparkR::sql("select 
                                case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end as season_bin, count(Issue_Date) as ticket_freq
                                from df_view_2017
                               group by case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end
                               order by 2 desc")
head(season_bin_2017,20)

# visual analysis
season_bin_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY_2015",nrow(season_bin_2015)),head(season_bin_2015,nrow(season_bin_2015)))),
                                        (cbind("FY_Year" = rep("FY_2016",nrow(season_bin_2016)),head(season_bin_2016,nrow(season_bin_2016)))),
                                        (cbind("FY_Year" = rep("FY_2017",nrow(season_bin_2017)),head(season_bin_2017,nrow(season_bin_2017))))))

head(season_bin_overall,nrow(season_bin_overall))

ggplot(data = season_bin_overall,aes(x = factor(season_bin),y = ticket_freq)) +
  geom_col(col = "black", fill = "dodgerblue",width = 0.5) + xlab("Season") + ylab("Ticket Frequency")+
  facet_grid('FY_Year')
# Then, find the 3 most common violations for each of these season

violation_season_bin_2015 <- SparkR::sql("select season_bin,Violation_Code,violation_freq
                              from (select season_bin,Violation_Code,violation_freq,
                                  dense_rank() over(partition by season_bin order by violation_freq desc) as d_rank
                                from (select 
                                case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end as season_bin, Violation_Code,count(Violation_Code) as violation_freq
                                from df_view_2015
                               group by case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end,Violation_Code))
                               where d_rank <= 3")
head(violation_season_bin_2015,20)

violation_season_bin_2016 <- SparkR::sql("select season_bin,Violation_Code,violation_freq
                              from (select season_bin,Violation_Code,violation_freq,
                                  dense_rank() over(partition by season_bin order by violation_freq desc) as d_rank
                                from (select 
                                case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end as season_bin, Violation_Code,count(Violation_Code) as violation_freq
                                from df_view_2016
                               group by case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end,Violation_Code))
                               where d_rank <= 3")
head(violation_season_bin_2016,20)

violation_season_bin_2017 <- SparkR::sql("select season_bin,Violation_Code,violation_freq
                              from (select season_bin,Violation_Code,violation_freq,
                                  dense_rank() over(partition by season_bin order by violation_freq desc) as d_rank
                                from (select 
                                case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end as season_bin, Violation_Code,count(Violation_Code) as violation_freq
                                from df_view_2017
                               group by case when month(Issue_Date) in (3,4,5) then 'spring'
                                    when month(Issue_Date) in (6,7,8) then 'summer'
                                    when month(Issue_Date) in (9,10,11) then 'fall'
                                    when month(Issue_Date) in (12,1,2) then 'winter'
                                end,Violation_Code))
                               where d_rank <= 3")
head(violation_season_bin_2017,20)

# visual analysis
violation_season_bin_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY_2015",nrow(violation_season_bin_2015)),head(violation_season_bin_2015,nrow(violation_season_bin_2015)))),
                                                    (cbind("FY_Year" = rep("FY_2016",nrow(violation_season_bin_2016)),head(violation_season_bin_2016,nrow(violation_season_bin_2016)))),
                                                    (cbind("FY_Year" = rep("FY_2017",nrow(violation_season_bin_2017)),head(violation_season_bin_2017,nrow(violation_season_bin_2017))))))

head(violation_season_bin_overall,nrow(violation_season_bin_overall))

ggplot(data = violation_season_bin_overall,aes(x = factor(season_bin),y = violation_freq,fill=as.factor(Violation_Code))) +
  geom_col(col = "black", width = 0.5) + xlab("Season") + ylab("Frequency")+
  facet_grid('FY_Year')
#----------------------------------------------------------------------------------------
# 7. The fines collected from all the parking violation constitute a revenue source for the NYC police department. 
# Let’s take an example of estimating that for the 3 most commonly occurring codes.
# Find total occurrences of the 3 most common violation codes

# ----------------analysis observation -------------------------------
# Year 2015 top 3 violation code - 21,38,14
# Year 2016 top 3 violation code - 21,36,38
# Year 2017 top 3 violation code - 21,36,38

violation_freq_2015 <- SparkR::sql("select Violation_Code,count(*) as freq
                                            from df_view_2015
                                            group by Violation_Code
                                            order by 2 desc")

head(violation_freq_2015,3)

violation_freq_2016 <- SparkR::sql("select Violation_Code,count(*) as freq
                                            from df_view_2016
                                            group by Violation_Code
                                            order by 2 desc")

head(violation_freq_2016,3)

violation_freq_2017 <- SparkR::sql("select Violation_Code,count(*) as freq
                                            from df_view_2017
                                            group by Violation_Code
                                            order by 2 desc")
                                            
head(violation_freq_2017,3)

#Visual analysis
violation_freq_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY-2015",3),head(violation_freq_2015,3))),
                                           (cbind("FY_Year" = rep("FY-2016",3),head(violation_freq_2016,3))),
                                           (cbind("FY_Year" = rep("FY-2017",3),head(violation_freq_2017,3)))))

head(violation_freq_overall,9)

ggplot(data = violation_freq_overall,aes(x = factor(Violation_Code),y = freq)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Violation Code") + ylab("Frequency")+
  facet_grid('FY_Year')

# Then, search the internet for NYC parking violation code fines. 
# You will find a website (on the nyc.gov URL) that lists these fines. 
# They’re divided into two categories, one for the highest-density locations of the city, the other for the rest of the city. 
# For simplicity, take an average of the two.

# <violation code>  <high density fine amount>  <other areas> <avg. fine>
# 21                $65                          $45            $55
# 36                $50                          $50            $50
# 38                $65                          $35            $50
# 14                $115                         $115           $115

# Using this information, find the total amount collected for all of the fines. 
# State the code which has the highest total collection.

violation_fine_2015 <- SparkR::sql("select Violation_Code,
                                  case when Violation_Code = 21 then count(Violation_Code)*55
                                  when Violation_Code = 38 then count(Violation_Code)*50
                                   when Violation_Code = 14 then count(Violation_Code)*115
                                  end as fine_amt
                                            from df_view_2015
                                              where Violation_Code in (21,38,14)
                                            group by Violation_Code
                                            order by 2 desc")

head(violation_fine_2015,3)

violation_fine_2016 <- SparkR::sql("select Violation_Code,
                                  case when Violation_Code = 21 then count(Violation_Code)*55
                                  when Violation_Code = 38 then count(Violation_Code)*50
                                   when Violation_Code = 36 then count(Violation_Code)*50
                                  end as fine_amt
                                            from df_view_2016
                                              where Violation_Code in (21,36,38)
                                            group by Violation_Code
                                            order by 2 desc")

head(violation_fine_2016,3)

violation_fine_2017 <- SparkR::sql("select Violation_Code,
                                  case when Violation_Code = 21 then count(Violation_Code)*55
                                  when Violation_Code = 38 then count(Violation_Code)*50
                                   when Violation_Code = 36 then count(Violation_Code)*50
                                  end as fine_amt
                                            from df_view_2017
                                              where Violation_Code in (21,36,38)
                                            group by Violation_Code
                                            order by 2 desc")

head(violation_fine_2017,3)

#Visual analysis
violation_fine_overall <- data.frame(rbind((cbind("FY_Year" = rep("FY-2015",3),head(violation_fine_2015,3))),
                                           (cbind("FY_Year" = rep("FY-2016",3),head(violation_fine_2016,3))),
                                           (cbind("FY_Year" = rep("FY-2017",3),head(violation_fine_2017,3)))))

head(violation_fine_overall,9)

ggplot(data = violation_fine_overall,aes(x = factor(Violation_Code),y = fine_amt)) +
  geom_col(col = "black", fill = "dodgerblue", width = 0.5) + xlab("Violation Code") + ylab("Fine Amount")+
  facet_grid('FY_Year')

# What can you intuitively infer from these findings?
# We notice that majority of parking tickets issued are for parking in No Parking zones(21),
# expired time on Muni-Meter receipts(37-38) and Exceeding the mentioned speed limit near school 
# zones(36). 
# We infer that there are a huge number of cars owned by the people and the nature of the 
# violation codes point to the lack of parking space in NYC. The existing space is not adequate to 
# meet the demand and hence more parking space is needed.




