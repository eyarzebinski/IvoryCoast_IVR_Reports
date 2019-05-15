##############################################
#### before you run this code, read below ####

# 1. open connection to eneza database
# 2. navigate to interactions_SQL.py and run in command prompt / terminal
# 3. navigate to cdr_SQL.py and run in command prompt / terminal
# 4. run all code chunks in TRECC_datasetGeneration_fullStudy.Rmd <-- DO NOT KNIT, JUST CLICK "RUN ALL"
# 5. in this R script (dataPrep), change dataDate to the most recent data date that you want to run.\
# 6. call this R script from either TRECC_IVR_PerformanceAnalyses.Rmd or TRECC_IVR_fullStudy_UsageAnalysis.Rmd as appropriate; it is designed to work for both.

#You will also need the following csvs for the code to run properly. These will not be shared on github for privacy reasons.
# 1. phoneNumberDemographicMapping.csv
# 2. testingNumbers.csv
# 3. childId_adultPhone_20190415.csv
# 4. Phone Contact Input Template.csv

######################################################################################
#### setup ####

####CHANGE THIS EACH TIME TO POINT TO THE RIGHT FILES####
#set file date
dataDate = "2019-05-15"

#bad ids are testing accounts or duplicates of valid numbers confirmed not to be used.
badIds = c(1, 2, 3, 44, 45, 46, 51, 53, 54, 56, 57, 64, 66, 68, 69, 74, 105, 147, 148, 149, 150, 192, 193, 194, 196, 238, 239, 240, 988)

#load libraries
library(tidyverse)
library(anytime)
library(jsonlite)
library(DT)
library(chron)
library(lubridate)
library(DBI)
library(dbplyr)
library(RMariaDB)
library(janitor)
library(reticulate)
library(rlist)
library(gtools)
library(plotly)

#raw data cannot go on github due to mobile numbers in raw data
#store it somewhere else local on your machine and call it

#conditional setwd
ifelse((Sys.info()["sysname"] == "windows"),
       #set for windows
       setwd("D:/Ivory Coast/201901 Study/"),
       #set for mac
       setwd("/Volumes/OganData/Ivory Coast/201901 Study"))

#load UAS
UASdata = read.csv2(file = paste0("UAS_",dataDate,".csv"), stringsAsFactors = F)

#load cdr
cdrData = read.csv2(file = paste0("cdr_concat_",dataDate,".csv"), stringsAsFactors = F)

#load interactions
interactionsData = read.csv2(file = paste0("interactions_concat_",dataDate,".csv"), stringsAsFactors = F)

#load users
treccUserData = read.csv2(file = paste0("users_",dataDate,".csv"), stringsAsFactors = F)

#load phone number to village and school mapping
childUsers = read.csv("phoneNumberDemographicMapping.csv",header=TRUE)
adultPhoneMapping = read.csv("childId_adultPhone_20190415.csv", stringsAsFactors = F)

#load University of Delaware Redcap ID mapping
redcapID = read.csv("Phone Contact Input Template.csv", stringsAsFactors = F)

#load testingNumbers
testingNumbers = read.csv("testingNumbers.csv", header = F, stringsAsFactors = F)

######################################################################################

#prep redcapId
redcapID = redcapID %>%
  select(portable_id, record_id) %>%
  mutate(row = 1) %>%
  filter(!is.na(portable_id),
         portable_id != 108,
         portable_id != 166,
         portable_id != 196) %>%
  rename(studyPhoneId = portable_id,
         redcapId = record_id)

#Basic cdr processing to find correct user counts
cdrData = as_tibble(cdrData) %>%
  filter(!src %in% testingNumbers$V1) %>%
  mutate(date = as.Date(substr(calldate, 1, 10)),
         time = substr(calldate, 12, 19),
         dateTime = as.POSIXct(strptime(paste(date, " ", time), "%Y-%m-%d %H:%M:%S")),
         hourExtract = hour(as.character(dateTime)),
         minuteExtract = minute(as.character(dateTime)),
         secondExtract = second(as.character(dateTime)),
         weekNumber_study = lubridate::isoweek(date)-5,
         dupeChecker = ifelse(clid == lag(clid), 1, 0),
         callType = ifelse(dcontext == "MTNCI" & src != 22012349, "user-initiated call",
                           ifelse(dcontext == "callback_mtn" | dst == "callback_mtn", "ivr callback",
                                  ifelse(dcontext == "outbound_mtn", "auto-generated call",
                                         ifelse(src == dst & dst == 22012349, "ignore",
                                                ifelse(dst == "", "ignore",
                                                       ifelse(dcontext == "LAUNCH", "ignore","CHECK THIS"))))))) %>%
  filter(dupeChecker == 0)

#these are dupes 
dupes_cdr = cdrData %>%
  group_by(clid,date) %>%
  summarize(n = n()) %>%
  filter(n > 1)

#create auto-generated table
cdrData_autoGenerate = cdrData %>%
  filter(callType == "auto-generated call")
  # mutate(users.localNumber = as.numeric(dst)) %>%
  # left_join(treccUserData, by = c("users.localNumber")) %>%
  # mutate(treccId_phoneId = paste0(treccUserId,"_",studyPhoneId)) %>%
  # filter(users.localNumber != 57101759 & users.localNumber != 75427722 & users.localNumber != 09250007 & users.localNumber != 14129614447)

#create user-initiated / callback table
cdrData_userCalls = cdrData %>%
  filter(callType == "ivr callback" | callType == "user-initiated call")
  # mutate(dstFix = ifelse(dst == "callback_mtn", substr(dstchannel, 7, 14), dst),
  #        dstFix = as.numeric(dstFix),
  #        users.localNumber = ifelse(callType == "user-initiated call", src, dstFix),
  #        users.localNumber = as.numeric(users.localNumber)) %>%
  # left_join(treccUserData, by = c("users.localNumber")) %>%
  # mutate(treccId_phoneId = paste0(treccUserId,"_",studyPhoneId)) %>%
  # filter(users.localNumber != 57101759 & users.localNumber != 75427722 & users.localNumber != 09250007 & users.localNumber != 14129614447)

#create table of all users who initated a call to the ivr at least once
usersCalled_cdr = cdrData_userCalls %>%
  filter(callType == "user-initiated call") %>%
  group_by(src) %>%
  summarize(n = n()) %>%
  mutate(n = NULL) %>%
  rename(mobileNumberShort = src)

####################################
#### BASIC PROCESSING FOR USERS ####

#create table that excludes bad ids and gets the phone number properly formatted
#warnings here ok - they come from the character string "anonymous" being coerced to a numeric in mobile_number
treccUserData = as_tibble(treccUserData) %>%
  filter(!id %in% badIds,
         !mobile_number %in% testingNumbers) %>%
  mutate(mobileNumberLong = paste0(225,str_sub(mobile_number,-8)),
         mobileNumberShort = as.numeric(str_sub(mobileNumberLong,-8)),
         mobileNumberLong = as.numeric(mobileNumberLong)
         #realUser = ifelse(mobileNumberShort %in% usersCalled_cdr$mobile_number, 1, 0)
         #realUser = ifelse(id %in% usersCalled_interactions$userId, 1, 0)
  ) %>%
  #filter(realUser == 1) %>%
  rename(userType = user_type,
         treccUserId = id) %>%
  select(treccUserId, mobileNumberShort, mobileNumberLong, userType)


#2. load adult phone mapping
# #this is the adult number that attended the training
#warnings here are ok - generally from numbers entered without high confidence by Delaware team (will add "?" at the end)
adultPhoneMapping = as_tibble(adultPhoneMapping) %>%
  filter(!is.na(users.localNumber)) %>%
  mutate(mobileNumberShort = as.numeric(users.localNumber),
         studyPhoneId = as.integer(studyPhoneId))

#3. load child phone mapping
# # #this is the adult number that attended the training
# adultPhoneMapping = as_tibble(adultPhoneMapping) %>%
#   filter(!is.na(users.localNumber)) %>%
#   mutate(users.localNumber = as.integer(users.localNumber),
#          studyPhoneId = as.integer(studyPhoneId))

#this is the absolute mapping for children
childUsers = as_tibble(childUsers) %>%
  filter(!is.na(Phone.Number),
         Distribuez. == "Y") %>%
  select(ID.Portable, Village, School, Phone.Number) %>%
  rename(studyPhoneId = ID.Portable,
         village = Village,
         school = School,
         mobileNumberShort = Phone.Number) %>%
  mutate(village = as.character(village),
         school = as.character(school),
         studyPhoneId = as.integer(studyPhoneId),
         intlPhoneNumber = paste0(225,mobileNumberShort)) %>%
  #realUser = ifelse(intlPhoneNumber %in% treccUserData$mobile_number, 1, 0)) %>%
  #filter(realUser == 1) %>%
  mutate(realUser = NULL,
         intlPhoneNumber = NULL,
         userRole = "child")


#if the value is not specified, replace it with "unknown"
childUsers$village[childUsers$village == ""] <- "unknown"
childUsers$school[childUsers$school == ""] <- "unknown"
childUsers$village[childUsers$village == "?"] <- "unknown"
childUsers$school[childUsers$school == "?"] <- "unknown"
childUsers$village[childUsers$village == "N"] <- "unknown"
childUsers$school[childUsers$school == "N"] <- "unknown"
childUsers$village[childUsers$village == "??"] <- "unknown"
childUsers$school[childUsers$school == "??"] <- "unknown"
childUsers[c("village")][is.na(childUsers[c("village")])] <- "unknown"
childUsers[c("school")][is.na(childUsers[c("school")])] <- "unknown"
childUsers[c("studyPhoneId")][is.na(childUsers[c("studyPhoneId")])] <- "unknown"

#4. match treccuserdata$phonenumber with adultphonemapping$phonenumber.
treccUserData_adult = treccUserData %>%
  left_join(adultPhoneMapping, by = c("mobileNumberShort")) %>%
  rename(studyPhoneId_adult = studyPhoneId,
         userRole_adult = userRole) %>%
  mutate(users.localNumber = NULL)
  
#str(treccUserData_adult)

#5. if match, output study phone id
#(done above)

#join tables to create:
#  -trecc id
#  -study id
#  -treccId_phoneId (do at the end)
#  -role
#  -phone #

#(done above)

#6. join child phone mapping to the above table via study id

#figure out right order for this...

treccUserData_adult_child = treccUserData_adult %>%
  left_join(childUsers, by = c("mobileNumberShort")) %>%
  rename(userRole_child = userRole,
         studyPhoneId_child = studyPhoneId) %>%
  mutate(mobileNumberShort_fix = ifelse(nchar(mobileNumberShort) == 7, paste0(0,mobileNumberShort), mobileNumberShort),
         mobileNumberShort = NULL) %>%
  rename(mobileNumberShort = mobileNumberShort_fix) %>%
  mutate(userRole = ifelse(!is.na(userRole_adult), userRole_adult, userRole_child),
         studyPhoneId = ifelse(!is.na(studyPhoneId_adult), studyPhoneId_adult, studyPhoneId_child),
         userRole_adult = NULL,
         userRole_child = NULL,
         studyPhoneId_adult = NULL,
         studyPhoneId_child = NULL
         )

treccUserData_adult_child[c("studyPhoneId")][is.na(treccUserData_adult_child[c("studyPhoneId")])] <- 0
treccUserData_adult_child[c("village")][is.na(treccUserData_adult_child[c("village")])] <- "unknown"
treccUserData_adult_child[c("school")][is.na(treccUserData_adult_child[c("school")])] <- 0
treccUserData_adult_child[c("studyPhoneId")][is.na(treccUserData_adult_child[c("studyPhoneId")])] <- 0
treccUserData_adult_child$school[treccUserData_adult_child$school == ""] <- 0
treccUserData_adult_child[c("userRole")][is.na(treccUserData_adult_child[c("userRole")])] <- "unknown"

#6b. combine both study ids
#6c. remap childUsers via study id to get the village for children and adults.
#6d. add in redcap ids
childUsers = childUsers %>%
  select(studyPhoneId, village, school) %>%
  mutate(village_school = paste0(village,"_",school))

treccUserData_adult_child = treccUserData_adult_child %>%
  left_join(childUsers, by = c("studyPhoneId")) %>%
  mutate(school.x = NULL,
         village.x = NULL) %>%
  rename(school = school.y,
         village = village.y) %>%
  mutate(treccId_phoneId = paste0(treccUserId,"_",studyPhoneId)) %>%
  left_join(redcapID, by = c("studyPhoneId"))

#7. keep real users only (ie show up in cdr table)
treccUserData_adult_child_realCallers = treccUserData_adult_child %>%
  filter(mobileNumberShort %in% usersCalled_cdr$mobileNumberShort)

treccUserData_adult_child_realCallers[c("studyPhoneId")][is.na(treccUserData_adult_child_realCallers[c("studyPhoneId")])] <- 0
treccUserData_adult_child_realCallers[c("village")][is.na(treccUserData_adult_child_realCallers[c("village")])] <- "unknown"
treccUserData_adult_child_realCallers[c("school")][is.na(treccUserData_adult_child_realCallers[c("school")])] <- 0
treccUserData_adult_child_realCallers[c("studyPhoneId")][is.na(treccUserData_adult_child_realCallers[c("studyPhoneId")])] <- 0
treccUserData_adult_child_realCallers$school[treccUserData_adult_child_realCallers$school == ""] <- 0
treccUserData_adult_child_realCallers[c("userRole")][is.na(treccUserData_adult_child_realCallers[c("userRole")])] <- "unknown"

rm(treccUserData)

#old version####
# 
# #create table of all users who initated a call to the ivr at least once
# usersCalled_cdr = cdrData_userCalls %>%
#   filter(callType == "user-initiated call") %>%
#   group_by(src) %>%
#   summarize(n = n()) %>%
#   mutate(n = NULL) %>%
#   rename(mobileNumberShort = src)
# 
# #report length
# message("users who initiated calls: ", nrow(usersCalled_cdr))
# 
# ## uncomment to check user counts throughtout the filtering process - don't need for regular processing
# # usersCalled_cdrIVRcallback = cdrData %>%
# #   filter(callType == "ivr callback") %>%
# #   group_by(dst) %>%
# #   summarize(n = n()) %>%
# #   mutate(n = NULL)
# # 
# # message("users that the ivr called back: ", nrow(usersCalled_cdrIVRcallback))
# # 
# # usersCalled_cdrIVRcallbackAnswered = cdrData %>%
# #   filter(callType == "ivr callback" & disposition == "ANSWERED") %>%
# #   group_by(dst) %>%
# #   summarize(n = n()) %>%
# #   mutate(n = NULL)
# # 
# # message("users that answered an ivr callback: ", nrow(usersCalled_cdrIVRcallbackAnswered))
# # 
# # #make a list of users in Interactions - these are all users that have actually called according to Michel
# # usersCalled_interactions = as_tibble(interactionsData) %>%
# #   group_by(interactions.user_id) %>%
# #   summarize(n = n()) %>%
# #   mutate(n = NULL) %>%
# #   rename(userId = interactions.user_id)
# #   #filter(userId >= 241)
# # 
# # message("users in the interactions table: ", nrow(usersCalled_interactions))
# #   
# # # #make a list of users in cdr - these are all users that have actually called?
# # # usersCalled_cdrUserCalls = cdrData_userCalls %>%
# # #   group_by(src) %>%
# # #   summarize(n = n()) %>%
# # #   mutate(n = NULL,
# # #          mobile_number = as.numeric(paste0(225,src))) %>%
# # #   left_join(treccUserData, by = c(mobile_number)) %>%
# # #   select(id)
# # 
# # # #make a list of users in cdr - these are all users that have actually called?
# # # usersCalled_cdrAutoGen = cdrData_autoGenerate %>%
# # #   group_by(src) %>%
# # #   summarize(n = n()) %>%
# # #   mutate(n = NULL,
# # #          mobile_number = as.numeric(paste0(225,src))) %>%
# # #   left_join(treccUserData, by = c(mobile_number)) %>%
# # #   select(id)
# # checkUsers = usersCalled_cdr %>%
# #   mutate(notInInteractions = ifelse(userId %in% usersCalled_interactions, 1, 0))
# 
# #this is all users who have called in
# #NA by coercion error will throw for numbers that show up as "anonymous" - this will filter them out.
# 
# #bad ids are testing accounts
# badIds = c(1, 44, 45, 46, 51, 74, 105, 147, 148, 149, 150, 192, 193, 194, 196, 238, 239, 240)
# 
# #create table that excludes bad ids and gets the phone number properly formatted
# #warnings here ok - they come from the character string "anonymous" being coerced to a numeric in mobile_number
# treccUserData = as_tibble(treccUserData) %>%
#   filter(!id %in% badIds) %>%
#   mutate(mobile_number = paste0(225,str_sub(mobile_number,-8)),
#          mobileNumberShort = str_sub(mobile_number,-8),
#          mobile_number = as.numeric(mobile_number)
#          #realUser = ifelse(mobileNumberShort %in% usersCalled_cdr$mobile_number, 1, 0)
#        #realUser = ifelse(id %in% usersCalled_interactions$userId, 1, 0)
#   ) %>%
#   #filter(realUser == 1) %>%
#   select(id, mobile_number, user_type) %>%
#   rename(treccUserId = id)
# 
# # #this is the adult number that attended the training
# adultPhoneMapping = as_tibble(adultPhoneMapping) %>%
#    filter(!is.na(users.localNumber)) %>%
#    mutate(users.localNumber = as.integer(users.localNumber),
#           studyPhoneId = as.integer(studyPhoneId))
# 
# #this is the absolute mapping for children
# childUsers = as_tibble(childUsers) %>%
#   filter(!is.na(Phone.Number)) %>%
#   select(ID.Portable, Village, School, Phone.Number) %>%
#   rename(studyPhoneId = ID.Portable,
#          village = Village,
#          school = School,
#          mobile_number = Phone.Number) %>%
#   mutate(village = as.character(village),
#          school = as.character(school),
#          studyPhoneId = as.integer(studyPhoneId),
#          intlPhoneNumber = paste0(225,mobile_number)) %>%
#          #realUser = ifelse(intlPhoneNumber %in% treccUserData$mobile_number, 1, 0)) %>%
#   #filter(realUser == 1) %>%
#   mutate(realUser = NULL,
#          intlPhoneNumber = NULL)
# 
# #if the value is not specified, replace it with "unknown"
# childUsers$village[childUsers$village == ""] <- "unknown"
# childUsers$school[childUsers$school == ""] <- "unknown"
# childUsers$village[childUsers$village == "?"] <- "unknown"
# childUsers$school[childUsers$school == "?"] <- "unknown"
# childUsers$village[childUsers$village == "N"] <- "unknown"
# childUsers$school[childUsers$school == "N"] <- "unknown"
# childUsers$village[childUsers$village == "??"] <- "unknown"
# childUsers$school[childUsers$school == "??"] <- "unknown"
# childUsers[c("village")][is.na(childUsers[c("village")])] <- "unknown"
# childUsers[c("school")][is.na(childUsers[c("school")])] <- "unknown"
# childUsers[c("studyPhoneId")][is.na(childUsers[c("studyPhoneId")])] <- "unknown"
# 
# #v01
# #removes mobile number
# users = childUsers %>%
#   ungroup() %>%
#   mutate(mobile_number = as.numeric(paste0(225,mobile_number))) %>%
#   left_join(treccUserData, by = c("mobile_number")) %>%
#   filter(!is.na(treccUserId)) %>%
#   mutate(treccUserId = as.integer(ifelse(is.na(treccUserId), NA, treccUserId)),
#          treccId_phoneId = paste0(treccUserId, "_",studyPhoneId),
#          mobile_number = NULL)
# 
# # #v02
# # #keeps mobile number
# # users = childUsers %>%
# #   ungroup() %>%
# #   mutate(mobile_number = as.numeric(as.character(paste0("225",mobile_number)))) %>%
# #   left_join(treccUserData, by = c("mobile_number")) %>%
# #   mutate(treccUserId = as.integer(ifelse(is.na(treccUserId), NA,treccUserId)))
# 
# 
# #prep data for joining to users and adult phone tables, then join them
# treccUserData = treccUserData %>%
#   mutate(treccUserId = as.numeric(treccUserId)) %>%
#   left_join(users, by = c("treccUserId")) %>%
#   mutate(userRole = ifelse(is.na(studyPhoneId), "unknown", "child"),
#          localPhoneNumber = as.integer(substr(mobile_number, 4, 99))) %>%
#   rename(users.internationalNumber = mobile_number,
#          users.localNumber = localPhoneNumber) %>%
#   select(treccUserId,studyPhoneId,users.internationalNumber, users.localNumber,village,school,userRole) %>%
#   mutate(treccUserId = as.numeric(as.character(treccUserId)),
#          village = as.character(village),
#          school = as.character(school)) %>%
#   left_join(adultPhoneMapping, by = c("users.localNumber")) %>%
#   mutate(studyPhoneId = ifelse(!is.na(studyPhoneId.y), studyPhoneId.y, studyPhoneId.x),
#          studyPhoneId.x = NULL,
#          studyPhoneId.y = NULL,
#          studyPhoneId = as.integer(studyPhoneId),
#          userRole = ifelse(!is.na(userRole.y), userRole.y, userRole.x),
#          userRole.x = NULL,
#          userRole.y = NULL,
#          village = NULL,
#          school = NULL)
#   
# treccUserData[c("studyPhoneId")][is.na(treccUserData[c("studyPhoneId")])] <- 0
# 
# treccUserData = treccUserData %>%
#   mutate(treccId_phoneId = paste0(treccUserId,"_",studyPhoneId), treccId_phoneId)
# 
# users = users %>%
#   mutate(treccUserId = NULL)
# 
# treccUserData = treccUserData %>%
#   mutate(treccId_phoneId = NULL) %>%
#   left_join(users, by = c("studyPhoneId")) %>%
#   mutate(user_type = NULL,
#          treccId_phoneId = paste0(treccUserId,"_",studyPhoneId)) %>%
#   arrange(treccId_phoneId) %>%
#   mutate(extraId = ifelse(treccId_phoneId == lead(treccId_phoneId), 1, 0)) %>%
#   filter(extraId != 1) %>%
#   mutate(extraId = NULL)
#   
# treccUserData[c("village")][is.na(treccUserData[c("village")])] <- "unknown"
# treccUserData[c("school")][is.na(treccUserData[c("school")])] <- 0
# treccUserData[c("studyPhoneId")][is.na(treccUserData[c("studyPhoneId")])] <- 0
# treccUserData$school[treccUserData$school == ""] <- 0
# 
# #now keep only those who called cdr
# treccUserData = treccUserData %>%
#   mutate(realUser = ifelse(users.internationalNumber %in% treccUserData$mobile_number, 1, 0)) %>%
#   filter(realUser == 1)


##################################
#### BASIC PROCESSING FOR UAS ####
# main actions: 
# -join student study id into UAS
# -divide data into study phone users and unknown users

#merge in student study id
#open the TRECC PHASE 2 google doc and confirm the last edit date. Export if there are new edits.

#join usersData to UAS data
UASdata <- as_tibble(UASdata) %>%
  filter(!users.id %in% badIds,
         !sessions.mobile_number %in% testingNumbers,
         users.id != 987,
         users.id != 988) %>%
  rename(treccUserId = UAS.user_id) %>%
  mutate(treccUserId = as.numeric(treccUserId),
         date = as.Date(substr(UAS.created_at, 1, 10)),
         time = substr(UAS.created_at, 12, 19),
         dateTime = as.POSIXct(strptime(paste(date, " ", time), "%Y-%m-%d %H:%M:%S")),
         interventionDateRange = ifelse(date >= "2019-02-09", 1, 0),
         hourExtract = lubridate::hour(hms(as.character(time))),
         minuteExtract = lubridate::minute(hms(as.character(time))),
         secondExtract = lubridate::second(hms(as.character(time))),
         weekNumber_study = lubridate::isoweek(date)-5,
         cmsQuestions.question_text = as.character(cmsQuestions.question_text)) %>%
  filter(interventionDateRange == 1 & !sessions.mobile_number %in% testingNumbers) %>%
  left_join(treccUserData_adult_child_realCallers, by = c("treccUserId")) %>%
  mutate(treccId_phoneId = paste0(sessions.user_id,"_",studyPhoneId)) %>%
  arrange(sessions.mobile_number, dateTime)

dupes_UAS = UASdata %>%
  group_by(UAS.id, date) %>%
  summarize(n = n()) %>%
  filter(n > 1)


# #get mapping between question_id and lesson_id for reading into interactions below
# UASdata_questionLesson_mapping = UASdata %>%
#   group_by(users.id, date, UAS.question_id, UAS.lesson_id, hourExtract, minuteExtract) %>%
#   summarize(lessonCount = n_distinct(UAS.lesson_id)) %>%
#   rename(UAS.lessonId = UAS.lesson_id) %>%
#   ungroup() %>%
#   mutate(CONCAT_userId_date_questionId_hour_minute = paste0(users.id, date, UAS.question_id, hourExtract, minuteExtract)) %>%
#   select(CONCAT_userId_date_questionId_hour_minute, UAS.lessonId) 
# #%>%
#   #group_by(CONCAT_userId_date_questionId_hour_minute) %>%
#   #summarize(lessonCount = n_distinct(UAS.lessonId)) %>%
#   #filter(lessonCount > 1)
# 
# dupes_UASlessonMapping = UASdata_questionLesson_mapping %>%
#   group_by(CONCAT_userId_date_questionId_hour_minute) %>%
#   summarize(n = n()) %>%
#   filter(n > 1)
  
###########################################
#### BASIC PROCESSING FOR INTERACTIONS ####
# 
# # quick fix to interactionsData
# previousDataDate = as.Date(as.character(previousDataDate),format="%Y%m%d")
# 
# oldInteractionsData = oldInteractionsData %>%
#   mutate(date = as.Date(substr(interactions.date_created, 1, 10)))
# 
# # #comment this out when you hand-fix the interactions concat (usually from over the weekend)
# #         removeOldDate = ifelse(date>=previousDataDate, 1, 0)) %>%
# #  filter(removeOldDate != 1) %>%
# #  mutate(removeOldDate = NULL,
# #         date = NULL)
# 
# #bind oldInteractionsData to newInteractionsData
# #interactionsData = rbind(oldInteractionsData, newInteractionsData)
# interactionsData = oldInteractionsData

#output the bind to set as the "oldInteractionsData" for tomorrow's data run.
#write.csv2(interactionsDate, file = paste0("interactions_concat_",dataDate,".csv"), row.names = F)

#sort by studyPhoneId and DateTime, remove known testing accounts
interactionsData = as_tibble(interactionsData) %>%
  ungroup() %>%
  filter(!interactions.user_id %in% badIds) %>%
  mutate(date = as.Date(substr(as.Date(interactionData.created_at), 1, 10)),
         cdr.uniqueId = ifelse(interactionData.data_key == "UniqueID", interactionData.value, NA),
         UAS.questionId = ifelse(interactionData.data_key == "question_id", interactionData.value, NA),
         UAS.lessonId = ifelse(interactionData.data_key == "lesson_id", interactionData.value, NA),
         weekNumber_study = lubridate::isoweek(date)-5,
         time = substr(interactionData.created_at, 12, 19),
         dateTime = as.POSIXct(strptime(paste(date, " ", time), "%Y-%m-%d %H:%M:%S")),
         interventionDateRange = ifelse(date >= "2019-02-09", 1, 0),
         hourExtract = hour(hms(as.character(time))),
         minuteExtract = minute(hms(as.character(time))),
         secondExtract = second(hms(as.character(time))),
         interactions.user_id = as.numeric(interactions.user_id),
         cdr.uniqueId = ifelse(interactionData.data_key == "UniqueID", interactionData.value, NA),
         dupeChecker = ifelse(interactionData.id == lag(interactionData.id), 1, 0)
         ) %>%
  filter(dupeChecker == 0,
         !is.na(date)) %>%
  arrange(interactions.user_id, interactionData.id)

#test which lines were duplicated
dupes_interactionsData = interactionsData %>%
  group_by(interactionData.id, date) %>%
  #group_by(interactionData.id, interactionData.data_key) %>%
  summarize(n = n()) %>%
  filter(n > 1)

#test which lines were duplicated
dupes_users = treccUserData_adult_child %>%
  group_by(mobileNumberShort) %>%
  summarize(n = n()) %>%
  filter(n > 1)

#change all NULL values to NA in particular columns
#interactionsData$cdr.uniqueId[interactionsData$cdr.uniqueId == "NULL"] <- NA
#interactionsData$UAS.lessonId[interactionsData$UAS.lessonId == "NULL"] <- NA
#interactionsData$UAS.questionId[interactionsData$UAS.questionId == "NULL"] <- NA

##################################################################################################################
#try the questions version now from HERE

#now fill the NA uniqueId values with the last real value
interactionsData = interactionsData %>%
  group_by(interactions.user_id, date) %>%
  fill(cdr.uniqueId, .direction = "down") %>%
  fill(cdr.uniqueId, .direction = "up") %>%
  ungroup() %>%
  #mutate(CONCAT_userId_date_questionId_hour_minute = ifelse(!is.na(UAS.question_id), paste0(interactions.user_id, date, UAS.question_id, hourExtract, minuteExtract), "NULL")) %>%
  #left_join(UASdata_questionquestion_mapping, by = c("CONCAT_userId_date_questionId_hour_minute")) 
  mutate(questionId2 = UAS.questionId) %>%
  group_by(interactions.user_id, date) %>%
  fill(UAS.questionId, .direction = "down") %>%
  fill(UAS.questionId, .direction = "up") %>%
  filter(!is.na(cdr.uniqueId)) %>%
  ungroup() %>%
  rename(treccUserId = interactions.user_id) %>%  #added in from below, does this work here?
  mutate(treccUserId = as.numeric(treccUserId)) %>%
  filter(treccUserId != 987,
         treccUserId != 988) %>%
  left_join(treccUserData_adult_child_realCallers, by = c("treccUserId")) %>%  #added in from below, does this work here?
  mutate(CONCAT_question_treccStudyID_date_hour_minute_second = paste0(UAS.questionId,"_",treccId_phoneId,"_",date, "_", hourExtract,"_",minuteExtract,"_", secondExtract)) %>%
  group_by(treccUserId, date) %>%
  mutate(linkedChildId = ifelse(interactionData.data_key == "childId" & interactionData.value != treccUserId, interactionData.value, NA)) %>%
  fill(linkedChildId, .direction = "down") %>%
  fill(linkedChildId, .direction = "up") %>%
  mutate(village_school = paste0(village,"_",school))


#CHECK THIS. is it better to get really specific (down to the second?) or just do one general join? which has less dupes?
#create a table of uniqueIDs and questionIDs for a later join
interactionsData_questions_DHMS = interactionsData %>%
  filter(!is.na(UAS.questionId)) %>%
  group_by(treccId_phoneId, cdr.uniqueId, UAS.questionId, date, hourExtract, minuteExtract, secondExtract) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  mutate(CONCAT_question_treccIDStudyID_date_hour_minute_second = paste0(UAS.questionId,"_",treccId_phoneId,"_",date,"_",hourExtract,"_",minuteExtract,"_",secondExtract)) %>%
  select(CONCAT_question_treccIDStudyID_date_hour_minute_second, cdr.uniqueId) %>%
  rename(cdr.uniqueId_DHMS = cdr.uniqueId)

interactionsData_questions_DHM = interactionsData %>%
  filter(!is.na(UAS.questionId)) %>%
  group_by(treccId_phoneId, cdr.uniqueId, UAS.questionId, date, hourExtract, minuteExtract) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  mutate(CONCAT_question_treccIDStudyID_date_hour_minute = paste0(UAS.questionId,"_",treccId_phoneId,"_",date,"_",hourExtract,"_",minuteExtract)) %>%
  select(CONCAT_question_treccIDStudyID_date_hour_minute, cdr.uniqueId) %>%
  rename(cdr.uniqueId_DHM = cdr.uniqueId)

interactionsData_questions_DHMpartial = interactionsData %>%
  filter(!is.na(UAS.questionId)) %>%
  group_by(treccId_phoneId, cdr.uniqueId, UAS.questionId, date, hourExtract, minuteExtract) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  mutate(CONCAT_question_treccIDStudyID_date_hour_minutepartial = paste0(UAS.questionId,"_",treccId_phoneId,"_",date,"_",hourExtract,"_",substr(minuteExtract,0, 1))) %>%
  select(CONCAT_question_treccIDStudyID_date_hour_minutepartial, cdr.uniqueId) %>%
  rename(cdr.uniqueId_DHMpartial = cdr.uniqueId)

interactionsData_questions_DH = interactionsData %>%
  filter(!is.na(UAS.questionId)) %>%
  group_by(treccId_phoneId, cdr.uniqueId, UAS.questionId, date, hourExtract) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  mutate(CONCAT_question_treccIDStudyID_date_hour = paste0(UAS.questionId,"_",treccId_phoneId,"_",date,"_",hourExtract)) %>%
  select(CONCAT_question_treccIDStudyID_date_hour, cdr.uniqueId) %>%
  rename(cdr.uniqueId_DH = cdr.uniqueId)

interactionsData_questions_D = interactionsData %>%
  filter(!is.na(UAS.questionId)) %>%
  group_by(treccId_phoneId, cdr.uniqueId, UAS.questionId, date) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  mutate(CONCAT_question_treccIDStudyID_date = paste0(UAS.questionId,"_",treccId_phoneId,"_",date)) %>%
  select(CONCAT_question_treccIDStudyID_date, cdr.uniqueId) %>%
  rename(cdr.uniqueId_D = cdr.uniqueId)

#test which lines were duplicated
dupes_intquestion = interactionsData_questions_DHMS %>%
  group_by(CONCAT_question_treccIDStudyID_date_hour_minute_second, cdr.uniqueId_DHMS) %>%
  summarize(n = n()) %>%
  filter(n > 1)






#redundant when added select to H and HMS above
# #identify and delete lines that don't have a lessonID
# interactionsData_lessons_HMS$UAS.lessonId = as.character(interactionsData_lessons_HMS$UAS.lessonId)
# interactionsData_lessons_HMS[c("UAS.lessonId")][is.na(interactionsData_lessons_HMS[c("UAS.lessonId")])] <- "delete"
# interactionsData_lessons_HMS = interactionsData_lessons_HMS[!interactionsData_lessons_HMS$UAS.lessonId == "delete", ]
# 
# #reorder to put the concat first
# interactionsData_lessons_HMS = interactionsData_lessons_HMS %>%
#   select(CONCAT_lesson_treccIDStudyID_date_hour_minute_second,everything()) %>%
#   mutate(interactions.user_id = NULL,
#          UAS.lessonId = NULL,
#          count = NULL) %>%
#   #use this to run the code as intended
#   group_by(CONCAT_lesson_treccIDStudyID_date_hour_minute_second, cdr.uniqueId) %>%
#   #use this to see if each concat has a unique cdr id 
#   #group_by(CONCAT_lesson_treccStudyID) %>%
#   summarize(count = n()) 

#moved above (I think successfully??)
# #now join usersData to interactionsData
# #TODO why do I have to wait until now?
# interactionsData <- interactionsData %>%
#   rename(treccUserId = interactions.user_id) %>%
#   left_join(treccUserData, by = c("treccUserId"))

#now use this to join the unique ID into the UAS! (but catch up on the UAS processing first...)

###############################################################
#### remove all non-study dates and non-study Ids from UAS ####

#TODO here next!
#create a unique number for each row, then join interactionsData_questions by the question/id/date concat
UASdata = UASdata %>%
  ungroup() %>%
  tibble::rowid_to_column("uniqueRowIdentifier") %>%
  mutate(CONCAT_question_treccIDStudyID_date_hour_minute_second = paste0(UAS.question_id,"_",treccId_phoneId, "_",date,"_",hourExtract,"_",minuteExtract,"_", secondExtract),
         CONCAT_question_treccIDStudyID_date_hour_minute = paste0(UAS.question_id,"_",treccId_phoneId, "_",date,"_",hourExtract,"_",minuteExtract),
         CONCAT_question_treccIDStudyID_date_hour_minutepartial = paste0(UAS.question_id,"_",treccId_phoneId,"_",date,"_",hourExtract,"_",substr(minuteExtract,0, 1)),
         CONCAT_question_treccIDStudyID_date_hour = paste0(UAS.question_id,"_",treccId_phoneId, "_",date,"_",hourExtract),
         CONCAT_question_treccIDStudyID_date = paste0(UAS.question_id,"_",treccId_phoneId, "_",date),
         CONCAT_treccIdStudyId_date_hour_minute_second = paste0(treccId_phoneId, "_",date,"_",hourExtract,"_",minuteExtract,"_", secondExtract)
  ) %>%
  left_join(interactionsData_questions_DHMS, by = c("CONCAT_question_treccIDStudyID_date_hour_minute_second")) %>%
  mutate(keepDHMS = ifelse(!is.na(cdr.uniqueId_DHMS),1,0))

UASdata[c("cdr.uniqueId_DHMS")][is.na(UASdata[c("cdr.uniqueId_DHMS")])] <- 0

#confirm no dupes when keepDHMS == 1
dupes_UAS_DHMS = UASdata %>%
  filter(keepDHMS == 1) %>%
  group_by(uniqueRowIdentifier) %>%
  summarize(n = n()) %>%
  filter(n > 1)

#take UAS data, filter for cdr unique ids that were not resolved by DHMS, try to resolve via DHM
UASdata_DHMS0 = UASdata %>%
  filter(keepDHMS == 0) %>%
  left_join(interactionsData_questions_DHM, by = c("CONCAT_question_treccIDStudyID_date_hour_minute")) %>%
  mutate(keepDHM = ifelse(!is.na(cdr.uniqueId_DHM),1,0))

#take UAS data, filter for cdr unique ids that were not resolved by DHMS, try to resolve via DHM
UASdata_DHM0 = UASdata_DHMS0 %>%
  filter(keepDHM == 0) %>%
  left_join(interactionsData_questions_DHMpartial, by = c("CONCAT_question_treccIDStudyID_date_hour_minutepartial")) %>%
  mutate(keepDHMpartial = ifelse(!is.na(cdr.uniqueId_DHMpartial),1,0))

#confirm no dupes when keepDHM == 1
dupes_UAS_DHM = UASdata_DHMS0 %>%
  filter(keepDHM == 1) %>%
  group_by(uniqueRowIdentifier) %>%
  summarize(n = n()) %>%
  filter(n > 1)

#make a dataset with the uniqueRowIdentifier and cdr.uniqueId for joining in later
final_UAS_DHM = UASdata_DHMS0 %>%
  filter(keepDHM == 1) %>%
  select(uniqueRowIdentifier, cdr.uniqueId_DHM)

#take data not resolved by UAS_DHMS, try to resolve via DHM
UASdata_DHM0 = UASdata_DHMS0 %>%
  filter(keepDHM == 0) %>%
  left_join(interactionsData_questions_DH, by = c("CONCAT_question_treccIDStudyID_date_hour")) %>%
  mutate(keepDH = ifelse(!is.na(cdr.uniqueId_DH),1,0))

#confirm no dupes when keepDHM == 1
dupes_UAS_DH = UASdata_DHM0 %>%
  group_by(uniqueRowIdentifier) %>%
  summarize(n = n()) %>%
  filter(n > 1)

#make a dataset with the uniqueRowIdentifier and cdr.uniqueId
final_UAS_DH = UASdata_DHM0 %>%
  filter(keepDH == 1) %>%
  select(uniqueRowIdentifier, cdr.uniqueId_DH)

#merge final_UAS_DHM back into UASData, turn NAs into 0
#NOTE: this adds a few dupes but not that many
UASdata = UASdata %>%
  left_join(final_UAS_DHM, by = c("uniqueRowIdentifier"))
UASdata[c("cdr.uniqueId_DHM")][is.na(UASdata[c("cdr.uniqueId_DHM")])] <- 0

#UASdata_DHM0

#merge final_UAS_DH back into UASData
#NOTE: THIS ADDS HUNDREDS OF DUPES
UASdata = UASdata %>%
  left_join(final_UAS_DH, by = c("uniqueRowIdentifier"))
UASdata[c("cdr.uniqueId_DH")][is.na(UASdata[c("cdr.uniqueId_DH")])] <- 0

UASdata = UASdata %>%
  mutate(cdr.uniqueId = ifelse(cdr.uniqueId_DHMS != 0, cdr.uniqueId_DHMS,
                               ifelse(cdr.uniqueId_DHM != 0, cdr.uniqueId_DHM,
                                      ifelse(cdr.uniqueId_DH != 0, cdr.uniqueId_DH, 
                                             "CHECK THIS"))))

#TO HERE




UASdata = UASdata %>%
  #group_by(cdr.uniqueId, date) %>%
  #summarize(n = n()) %>%
  filter(!is.na(mobileNumberLong)) %>%
  mutate(cdr.uniqueId_DHMS = NULL,
         cdr.uniqueId_DHM = NULL,
         cdr.uniqueId_DH = NULL,
         keepDHMS = NULL,
         CONCAT_question_treccIDStudyID_date_hour_minute_second = NULL,
         CONCAT_question_treccIDStudyID_date_hour_minute = NULL,
         CONCAT_question_treccIDStudyID_date_hour = NULL,
         CONCAT_question_treccIDStudyID_date = NULL,
         village_school = paste0(village,"_",school))

# left_join(interactionsData_lessons_HM, by = c("CONCAT_lesson_treccIDStudyID_date_hour_minute")) %>%
#   mutate(keepHM = ifelse(keepHMS == 0 & !is.na(cdr.uniqueId_HM), 1, 0)) %>%
#   #now removed duped HM based on 1s
#   
#   left_join(interactionsData_lessons_H, by = c("CONCAT_lesson_treccIDStudyID_date_hour")) %>%
#   mutate(keepH = ifelse(keepHMS == 0 & keepHM == 0 & !is.na(cdr.uniqueId_H), 1, 0))
#   #now removed duped H based on 1s
# 
# #now fill the NA uniqueId values with the last real value
# UASdata = UASdata %>%
#   arrange(treccId_phoneId, dateTime) %>%
#   mutate(cdr.uniqueid2 = cdr.uniqueId,
#          BADCDR = ifelse(is.na(cdr.uniqueId), 1, 0)) %>%
#   group_by(treccId_phoneId, UAS.lesson_id) %>%
#   fill(cdr.uniqueId, .direction = "down") %>%
#   fill(cdr.uniqueId, .direction = "up")

#test which lines were duplicated
dupes_UAS = UASdata %>%
  group_by(UAS.id, date) %>%
  summarize(n = n()) %>%
  filter(n > 1)




















##################################
#### BASIC PROCESSING FOR CDR ####

# # moved above
# cdrData = cdrData %>%
#   filter(src != 57101759 & src != 75427722 & src != 09250007 & src != 14129614447) %>%
#   mutate(date = as.Date(substr(calldate, 1, 10)),
#          time = substr(calldate, 12, 19),
#          dateTime = as.POSIXct(strptime(paste(date, " ", time), "%Y-%m-%d %H:%M:%S")),
#          hourExtract = hour(as.character(dateTime)),
#          minuteExtract = minute(as.character(dateTime)),
#          weekNumber_study = lubridate::isoweek(date),
#          callType = ifelse(dcontext == "MTNCI" & src != 22012349, "user-initiated call",
#                            ifelse(dcontext == "callback_mtn" | dst == "callback_mtn", "ivr callback",
#                                   ifelse(dcontext == "outbound_mtn", "auto-generated call",
#                                          ifelse(src == dst & dst == 22012349, "ignore",
#                                                 ifelse(dst == "", "ignore",
#                                                        ifelse(dcontext == "LAUNCH", "ignore","CHECK THIS ROW")))))))

##merge users phoneNumber into interactions by treccId
#interactionsData = interactionsData %>%
  
#create index from interactions via date, treccId, and phone number
#use this index to merge back into cdr and determine which treccId called (will reduce cdr dupes)


# moved above
cdrData_autoGenerate = cdrData %>%
  filter(callType == "auto-generated call") %>%
  mutate(mobileNumberShort = as.character(dst)) %>%
  left_join(treccUserData_adult_child, by = c("mobileNumberShort")) %>%
  mutate(treccId_phoneId = paste0(treccUserId,"_",studyPhoneId)) %>%
  filter(!treccUserId %in% badIds,
         !mobileNumberShort %in% testingNumbers)

#test which lines were duplicated
dupes_interactionsData = interactionsData %>%
  group_by(interactionData.id, date) %>%
    #group_by(interactionData.id, interactionData.data_key) %>%
  summarize(n = n()) %>%
  filter(n > 1)






# 
# #it's actually ok if there are duplicates in the auto call because this is for the adul 
# cdrData_autoGenerate = cdrData_autoGenerate %>%
#   arrange(clid) %>%
#   mutate(checkDupe = ifelse(clid == lead(clid), 1, 0)) %>%
#   filter(checkDupe == 0)

cdrData_userCalls = as_tibble(cdrData) %>%
  filter(callType == "ivr callback" | callType == "user-initiated call") %>%
  mutate(dstFix = ifelse(dst == "callback_mtn", substr(dstchannel, 7, 14), dst),
         dstFix = as.numeric(dstFix),
         mobileNumberShort = ifelse(callType == "user-initiated call", src, dstFix),
         mobileNumberShort = as.character(mobileNumberShort)) %>%
  left_join(treccUserData_adult_child_realCallers, by = c("mobileNumberShort")) %>%
  mutate(treccId_phoneId = paste0(treccUserId,"_",studyPhoneId)) %>%
  filter(!treccUserId %in% badIds,
         !mobileNumberShort %in% testingNumbers)

# #test which lines were duplicated
# dupes_cdrUser = cdrData_userCalls %>%
#   group_by(clid) %>%
#   summarize(n = n()) %>%
#   filter(n > 1)
# 
# cdrData_userCalls = cdrData_userCalls %>%
#   arrange(clid) %>%
#   mutate(checkDupe = ifelse(clid == lead(clid), 1, 0)) %>%
#   filter(checkDupe == 0)

# suppressWarnings({
#   cdrData_old = cdrData_old %>%
#   mutate(date = as.Date(substr(calldate, 1, 10)),
#          time = substr(calldate, 12, 19),
#          dateTime = as.POSIXct(strptime(paste(date, " ", time), "%Y-%m-%d %H:%M:%S")),
#          hourExtract = hour(as.character(dateTime)),
#          minuteExtract = minute(as.character(dateTime)),
#          src = as.character(src),
#          users.localNumber = ifelse(str_length(src) != 11, as.integer(as.character(src)), as.integer(substr(src, 4, 99)))) %>%
#     left_join(treccUserData, by = c("users.localNumber"))
# })
# 
# suppressWarnings({
#   cdrData_new = cdrData_new %>%
#   mutate(date = as.Date(substr(calldate, 1, 10)),
#          time = substr(calldate, 12, 19),
#          dateTime = as.POSIXct(strptime(paste(date, " ", time), "%Y-%m-%d %H:%M:%S")),
#          hourExtract = hour(as.character(dateTime)),
#          minuteExtract = minute(as.character(dateTime)),
#          users.localNumber = as.integer(dst)) %>%
#   left_join(treccUserData, by = c("users.localNumber"))
# })
#   
#process cdr_new and bind
#
#split file into the following 2 cases, ignore all calls that don't match this criteria:
#
# #Case 1: User initate the call
# #NOTE: This is BOTH student AND parent phones
# #src = User phone number
# #dst = 22012349
# cdrData_old_userCalls = cdrData_old %>%
#   filter(dst == 22012349 & src != 57101759 & src != 75427722 & src != 09250007 & src != 22012349 & src != 220123549 & src != 14129614447)
# 
# cdrData_new_userCalls = cdrData_new %>%
#   filter(!is.na(treccUserId) & dcontext == "callback_mtn" & dst != 75427722 & dst != 57101759 & dst != 09250007 & dst != 22012349 & dst != 220123549 & dst != 14129614447) %>%
#   rename(dst1 = src,
#          src = dst) %>%
#   rename(dst = dst1) %>%
#   mutate(src = as.character(src),
#          users.localNumber = ifelse(str_length(src) != 11, as.integer(as.character(src)), as.integer(substr(src, 4, 99)))) %>%
#   select(clid, src, dst, everything())
# 
# cdrData_all_userCalls = rbind(cdrData_old_userCalls, cdrData_new_userCalls)
#
#Case 2: IVR calls to the User
#NOTE: This is BOTH student AND parent phones
#src = 22012349
#dst = User phone number
# suppressWarnings({
#   cdrData_old_systemCalls = cdrData_old %>%
#   #filter(is.na(studyPhoneId) & src == 22012349) %>%
#   filter(src == 22012349) %>%
#   select(-c(treccUserId, userRole, users.localNumber, users.internationalNumber, studyPhoneId, village, school)) %>%
#   mutate(users.localNumber = as.integer(dst))
# })
# 
# cdrData_new_systemCalls = cdrData_new %>%
#   #filter(is.na(studyPhoneId) & src == 22012349) %>%
#   filter(dcontext == "outbound_mtn" & src == 22012349) %>%
#   select(-c(treccUserId, userRole, users.localNumber, users.internationalNumber, studyPhoneId, village, school)) %>%
#   mutate(users.localNumber = as.integer(dst)) %>%
#   #rename(dst1 = src,
#   #       src = dst) %>%
#   #rename(dst = dst1) %>%
#   select(clid, src, dst, everything()) %>%
#   filter(users.localNumber != "callback_mtn") %>%
#   mutate(users.localNumber = as.integer(users.localNumber))
# 
# cdrData_all_systemCalls = rbind(cdrData_old_systemCalls, cdrData_new_systemCalls)
# 
# suppressWarnings(cdrData_all_systemCalls <- cdrData_all_systemCalls %>%
#                    left_join(treccUserData, by = c("users.localNumber")))

#sort by ID.Portable and dateTime for call numbering
cdrData_userCalls = dplyr::arrange(cdrData_userCalls, mobileNumberShort, dateTime)

#number calls in cdrData_all_userCalls
cdrData_userCalls = cdrData_userCalls %>%
  mutate(newWeek = ifelse(lag(weekNumber_study) != weekNumber_study, 1, 0)) %>%
  mutate(newCall = ifelse(lag(uniqueid) != uniqueid, 1, 0))
  
cdrData_userCalls[1, c("newWeek")] <- 1
cdrData_userCalls[1, c("newCall")] <- 1

cdrData_userCalls = cdrData_userCalls %>%
  arrange(mobileNumberShort, dateTime) %>%
  group_by(mobileNumberShort) %>%
  mutate(weekNumber_user = cumsum(newWeek),
         callNumber = cumsum(newCall),
         uniqueid = as.character(uniqueid)) %>%
  ungroup()








## after I fixed the interactionsData_lessons with DHMS, DHM, and Dh I don't think you need the below anymore...
# #clean up interactionsData_lessons and merge it into cdrData_userCalls
# interactionsData_lessons = interactionsData_lessons %>%
#   select(cdr.uniqueId, everything()) %>%
#   rename(uniqueid = cdr.uniqueId,
#          callHasUASData = count) %>%
#   mutate(uniqueid = as.character(uniqueid))
# 
# #this will duplicate some rows, but will be taken care of below
# cdrData_userCalls = cdrData_userCalls %>%
#   left_join(interactionsData_lessons, by = c("uniqueid"))
# 
# #fill in callHasUASData NAs with 0
# cdrData_userCalls[c("callHasUASData")][is.na(cdrData_userCalls[c("callHasUASData")])] <- 0


cdrData_userCalls = cdrData_userCalls %>%
  mutate(userRole = ifelse(is.na(studyPhoneId),"unknown","study phone")) %>%
  mutate(duplicateCall_V02 = ifelse(lag(clid) == clid, 1, 0))

cdrData_userCalls[1, c("duplicateCall_V02")] <- 0

cdrData_userCalls = cdrData_userCalls %>%
    filter(duplicateCall_V02 == 0)



#now fix auto-generate
#sort by ID.Portable and dateTime for call numbering
cdrData_autoGenerate = dplyr::arrange(cdrData_autoGenerate, mobileNumberShort, dateTime)

#number calls in cdrData_all_userCalls
cdrData_autoGenerate = cdrData_autoGenerate %>%
  mutate(newCall = ifelse(lag(uniqueid) != uniqueid, 1, 0))

cdrData_autoGenerate[1, c("newCall")] <- 1

cdrData_autoGenerate = cdrData_autoGenerate %>%
  arrange(mobileNumberShort, dateTime) %>%
  group_by(mobileNumberShort) %>%
  mutate(callNumber = cumsum(newCall),
         uniqueid = as.character(uniqueid)) %>%
  ungroup()

#create a table of uniqueIDs and lessonIDs for a later join
cdr_lessons_HMS = cdrData_userCalls %>%
  group_by(src, uniqueid, date, hourExtract, minuteExtract, secondExtract) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  mutate(CONCAT_src_date_hour_minute_second = paste0(src,"_",date,"_",hourExtract,"_",minuteExtract,"_",secondExtract)) %>%
  select(CONCAT_src_date_hour_minute_second, uniqueid) %>%
  rename(cdr.uniqueId_HMS = uniqueid)

#################################################################################################
#### table: child ID and cdr the student ID from interactions table, merge into other tables ####

#this will link adult and child numbers
interactionsData_childID_adultMapping = interactionsData %>%
  filter(interactionData.data_key == "childId") %>%
  select(cdr.uniqueId, interactionData.value) %>%
  group_by(cdr.uniqueId, interactionData.value) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  mutate(count = NULL,
         cdr.uniqueId = as.character(cdr.uniqueId)) %>%
  rename(uniqueid = cdr.uniqueId,
         childPhoneId = interactionData.value)

#join interactionsData_childID_adultMapping into cdr table
cdrData_userCalls <- cdrData_userCalls %>%
  left_join(interactionsData_childID_adultMapping, by = c("uniqueid"))

#not all of the cases are covered by the above for some reason. Try this way also to get the remaining NAs.
interactionsData_cdrUniqueId_childIDMapping = interactionsData %>%
  filter(interactionData.data_key == "UniqueID") %>%
  select(interactionData.value, treccUserId) %>%
  group_by(interactionData.value, treccUserId) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  mutate(count = NULL,
         interactionData.value = as.character(interactionData.value)) %>%
  rename(uniqueid = interactionData.value,
         childPhoneId_V02 = treccUserId)

#now map the adult ids to the child and process
cdrData_userCalls <- cdrData_userCalls %>%
  left_join(interactionsData_cdrUniqueId_childIDMapping, by = c("uniqueid")) %>%
  mutate(finalStudyUserId = ifelse(!is.na(studyPhoneId), studyPhoneId, NA),
         finalTreccUserId = ifelse(!is.na(treccUserId), treccUserId,
                                   ifelse(!is.na(childPhoneId), childPhoneId,
                                   ifelse(!is.na(childPhoneId_V02), childPhoneId_V02, NA))),
         uniqueid = as.character(uniqueid)) %>%
  select(-c(village, school, duplicateCall_V02, userRole, treccUserId, studyPhoneId, finalStudyUserId, mobileNumberShort, mobileNumberLong))

#now with the newly added treccIds, re-read in usersData to catch them
treccUserData_adult_child_realCallers = treccUserData_adult_child_realCallers %>%
  rename(finalTreccUserId = treccUserId)

#now join usersData to cdrData_old_userCalls
cdrData_userCalls <- cdrData_userCalls %>%
  mutate(treccId_phoneId = NULL,
         finalTreccUserId = as.numeric(as.character(finalTreccUserId))) %>%
  left_join(treccUserData_adult_child_realCallers, by = c("finalTreccUserId")) %>%
  mutate(treccId_phoneId = paste0(finalTreccUserId,"_",studyPhoneId))





#################################################
#### secondary processing in UAS data filter ####

UASdata$cmsQuestions.distractor_tokens[UASdata$cmsQuestions.distractor_tokens == "NULL"] <- NA
UASdata$cmsQuestions.text_output_structure[UASdata$cmsQuestions.text_output_structure == "NULL"] <- NA
UASdata$cmsQuestions.token_c_id[UASdata$cmsQuestions.token_c_id == "NULL"] <- NA
UASdata$cmsTokens_C.token_id_c[UASdata$cmsTokens_C.token_id_c == "NULL"] <- NA
UASdata$language_id_token_c[UASdata$language_id_token_c == "NULL"] <- NA
UASdata$open_closed_token_c[UASdata$open_closed_token_c == "NULL"] <- NA
UASdata$phonetics_auditory_token_c[UASdata$phonetics_auditory_token_c == "NULL"] <- NA
UASdata$simple_complex_token_c[UASdata$simple_complex_token_c == "NULL"] <- NA
UASdata$soundfile_name_token_c[UASdata$soundfile_name_token_c == "NULL"] <- NA
UASdata$spelling_visual_token_c[UASdata$spelling_visual_token_c == "NULL"] <- NA
UASdata$syllable_structure_token_c[UASdata$syllable_structure_token_c == "NULL"] <- NA
UASdata$token_type_id_token_c[UASdata$token_type_id_token_c == "NULL"] <- NA
UASdata$userTrialMastery.questions[UASdata$userTrialMastery.questions == "NULL"] <- NA
UASdata$userTrialMastery.questions_correct[UASdata$userTrialMastery.questions_correct == "NULL"] <- NA
UASdata$userTrialMastery.trial_mastery_score[UASdata$userTrialMastery.trial_mastery_score == "NULL"] <- NA
UASdata$userTrialMastery.unit_passed[UASdata$userTrialMastery.unit_passed == "NULL"] <- NA
UASdata$userTrialMastery.unit_passed[UASdata$userTrialMastery.unit_passed == "NULL"] <- NA
UASdata$userTrialMastery.completed[UASdata$userTrialMastery.completed == "NULL"] <- NA
UASdata$userTrialMastery.trial_started_at[UASdata$userTrialMastery.trial_started_at == "NULL"] <- NA
UASdata$userTrialMastery.trial_completed_at[UASdata$userTrialMastery.trial_completed_at == "NULL"] <- NA
UASdata$userTrialMastery.api_threshold[UASdata$userTrialMastery.api_threshold == "NULL"] <- NA
UASdata$userTrialMastery.api_questions[UASdata$userTrialMastery.api_questions == "NULL"] <- NA
#UASdata$userTrialMastery.max_questions_reached[UASdata$userTrialMastery.max_questions_reached == "NULL"] <- NA

#extract elapsed time (sec) per lesson since last line
UASdata = UASdata %>%
  ungroup() %>%
  arrange(sessions.mobile_number, dateTime) %>%
  mutate(changeCall = ifelse(cdr.uniqueId == lag(cdr.uniqueId), 0, 1),
         changeLesson = ifelse(UAS.lesson_id == lag(UAS.lesson_id), 0, 1),
         changeQuestion = ifelse(cmsQuestions.question_text == lag(cmsQuestions.question_text), 0, 1),
         changeAttempt = ifelse(changeQuestion == 0, 1, 0),
         changeWeek = ifelse(weekNumber_study == lag(weekNumber_study), 0, 1),
         elapsedTimeSec = ifelse(changeCall == 0, dateTime - lag(dateTime), NA))

#fix the first row for these columns due to lag()
UASdata[1, c("changeCall")] <- 1
UASdata[1, c("changeQuestion")] <- 1
UASdata[1, c("changeAttempt")] <- 0
UASdata[1, c("changeLesson")] <- 1
UASdata[1, c("elapsedTimeSec")] <- NA
UASdata[1, c("changeWeek")] <- 0

#use cumsum to number questions (per unit and overall) and attempts
#call number overall not reported here since that will be joined later via the cdr table
UASdata = UASdata %>%
  group_by(sessions.mobile_number) %>%
  mutate(questionNumberOverall = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(sessions.mobile_number, UAS.unit_id) %>%
  mutate(questionNumberPerUnit = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(sessions.mobile_number, UAS.unit_id, cmsQuestions.question_number) %>%
  mutate(questionNumberPerType = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(sessions.mobile_number, UAS.unit_id, questionNumberOverall) %>%
  mutate(attemptNumber = cumsum(changeAttempt),
         attemptNumber = attemptNumber + 1) %>%
  ungroup() %>%
  group_by(sessions.mobile_number) %>%
  mutate(lessonNumberOverall = cumsum(changeLesson)) %>%
  ungroup() %>%
  group_by(sessions.mobile_number,UAS.unit_id) %>%
  mutate(lessonNumberPerUnit = cumsum(changeLesson)) %>%
  ungroup() %>%
  group_by(sessions.mobile_number, lessonNumberOverall) %>%
  mutate(questionNumberPerLesson = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(sessions.mobile_number, cmsQuestions.trial_id) %>%
  mutate(questionNumberPerTrialId = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(sessions.mobile_number) %>%
  mutate(weekNumber_user = cumsum(changeWeek),
         weekNumber_user = weekNumber_user + 1) %>%
  ungroup()

#create mapping for reset lessons  
UAS_lessonDateReset = UASdata %>%
  group_by(treccId_phoneId, date, UAS.unit_id) %>%
  filter(!is.na(treccId_phoneId)) %>%
  summarize(n = n()) %>%
  ungroup() %>%
  mutate(resetUnit = ifelse(treccId_phoneId == lead(treccId_phoneId) & UAS.unit_id > lead(UAS.unit_id), 1, 0),
         userDateUnit_concat = paste0(treccId_phoneId,date,UAS.unit_id)) %>%
  filter(resetUnit == 1) %>%
  select(userDateUnit_concat)

#make V02 of co-occurring distractor token id (fix issue of "[" and "]" appearing only for units 3 and 4)
#create IPA for co-occurring distractors
#create visual spelling of co-occurring distractors
UASdata = UASdata %>%
  mutate(cmsQuestions.distractor_tokens_V02 = ifelse(is.na(cmsQuestions.token_c_id), "null", paste(cmsQuestions.token_b_id,", ",cmsQuestions.token_c_id, sep = "")),
         cmsQuestions.distractor_tokens_IPA = ifelse(is.na(cmsQuestions.token_c_id), "null", paste(phonetics_auditory_token_b,", ",phonetics_auditory_token_c, sep = "")),
         cmsQuestions.distractor_tokens_spelling = ifelse(is.na(cmsQuestions.token_c_id), "null", paste(spelling_visual_token_b,", ",spelling_visual_token_c, sep = "")),
         concat_lessonQuestion = paste0(UAS.lesson_id, UAS.question_id),
         userDateUnit_concat = paste0(treccId_phoneId,date,UAS.unit_id),
         unitReset = ifelse(userDateUnit_concat %in% UAS_lessonDateReset$userDateUnit_concat, 1, 0)
         ) %>%
  filter(unitReset != 1) %>%
  group_by(treccId_phoneId) %>%
  mutate(maxUnit = max(UAS.unit_id)) %>%
  group_by(treccId_phoneId, UAS.unit_id) %>%
  mutate(currentUnit = ifelse(UAS.unit_id != maxUnit, 0, 1))

#####################################
#### Secondary processing in CDR ####

cdr_callNumber = cdrData_userCalls %>%
  select(uniqueid,callNumber,finalTreccUserId) %>%
  group_by(uniqueid, callNumber, finalTreccUserId) %>%
  summarize(count = n()) %>%
  ungroup() %>%
  mutate(treccIDuniqueID = paste0(finalTreccUserId,uniqueid),
         count = NULL,
         finalTreccUserId = NULL,
         uniqueid = NULL)

#merge call number into interactions
interactionsData <- interactionsData %>%
  mutate(treccIDuniqueID = paste0(treccUserId, cdr.uniqueId)) %>%
  left_join(cdr_callNumber, by = c("treccIDuniqueID"))

#merge call number into UAS and filter out any tokens with a NULL value
UASdata <- UASdata %>%
  mutate(treccIDuniqueID = paste0(treccUserId, cdr.uniqueId)) %>%
  left_join(cdr_callNumber, by = c("treccIDuniqueID")) %>%
  filter(syllable_structure_token_a != "NULL")


cdrData_userCalls = cdrData_userCalls %>%
  filter(!is.na(mobileNumberLong))

######################################################################################################
#### Summarize multiple-attempts down into one row with First-attempt and Last-attempt accuracies ####

# First attempt statistics
UASdata.firstattempt <- UASdata[UASdata$attemptNumber==1,] %>%
  group_by(studyPhoneId,cmsQuestions.id) %>%
  summarize(
    FAcorrect = max(UAS.correct)
  ) 
# Last attempt statistics
UASdata.lastattempt <- UASdata %>%
  group_by(studyPhoneId,cmsQuestions.id) %>%
  summarise(
    LAcorrect = max(UAS.correct),
    attempts = max(attemptNumber)
  )
# Merge first and last attempt data
UASdata.questionid <- merge(UASdata.firstattempt,UASdata.lastattempt,by=c('studyPhoneId','cmsQuestions.id'))

# Record the number of answer options for each question (Y/N=2, A/B/C=3)
UASdata = as_tibble(UASdata) %>%
  mutate(options = ifelse(is.na(UASdata$cmsQuestions.token_c_id), 2, 3)) %>%
  filter(syllable_structure_token_a != "NULL")

#UASdata$options = 3
two_choice = is.na(UASdata$cmsQuestions.token_c_id)
#UASdata[two_choice,]$options = 2

# This function will be useful later for comparing 2-option and 3-option questions on equal footing
zbin <- function(acc,n_options){
  dist_mean = 1/n_options
  dist_sd = dist_mean * (1-dist_mean)
  zscr <- (acc-dist_mean)/dist_sd
  return(zscr)
}

# Descriptives for each question ID
UASdata.descriptives <- UASdata %>%
  group_by(studyPhoneId,cmsQuestions.id) %>%
  summarise(
    trialID = unique(cmsQuestions.trial_id),
    token_a_ID = unique(cmsQuestions.token_id),
    token_a_IPA = unique(phonetics_auditory_token_a),
    token_a_spelling = unique(spelling_visual_token_a),
    token_a_syllable_structure = unique(syllable_structure_token_a),
    options = mean(options),
    token_a_type = unique(token_type_id_token_a),
    trialDifficulty = unique(cmsQuestions.difficulty_level_trial),
    tokenDifficulty = mean(cmsQuestions.difficulty_level_token)
  )
UASdata.questionid <- merge(UASdata.questionid,UASdata.descriptives,by=c('studyPhoneId','cmsQuestions.id'))
UASdata.questionid$FAz = zbin(UASdata.questionid$FAcorrect,UASdata.questionid$options)
UASdata.questionid$LAz = zbin(UASdata.questionid$LAcorrect,UASdata.questionid$options)

######################################################################################################
#### Summarize by Token A ID ####
# This analysis excludes all of the Y/N (two-option) questions because students were not given multiple
# attempts with these questions. Getting only one attempt skews the accuracy ratings
#UASdata.tokenid <- UASdata.questionid[UASdata.questionid$options>2,] %>%
UASdata.tokenid <- UASdata.questionid %>%
    group_by(options, token_a_ID, token_a_IPA, token_a_spelling, token_a_syllable_structure, token_a_type, trialDifficulty, tokenDifficulty) %>%
  summarise(
    students = n_distinct(studyPhoneId),
    presentations = n(),
    meanAttempts = round(mean(attempts),2),
    FirstAcc = round(mean(FAcorrect),2),
    LastAcc = round(mean(LAcorrect),2),
    zFirstAcc = round(zbin(mean(FAcorrect),mean(options)),2),
    zLastAcc = round(zbin(mean(LAcorrect),mean(options)),2)
    #trialDifficulty = unique(trialDifficulty),
    #tokenDifficulty = unique(tokenDifficulty)
  )

#UASdata.tokenid.ALLtokens <- merge(cmsToken_table[,c('id','phonetics_auditory','syllable_structure')],UASdata.tokenid,by.y='tokenID',by.x='id',all.x=TRUE)
#UASdata.tokenid <- merge(cmsToken_table[,c('id','phonetics_auditory','syllable_structure')],UASdata.tokenid,by.y='tokenID',by.x='id')

# Summarize by Trial (Question-Template) ID
UASdata.trialid <- UASdata.questionid %>%
  group_by(options, trialID, trialDifficulty, tokenDifficulty) %>%
  summarise(
    students = n_distinct(studyPhoneId),
    presentations = n(),
    #options = round(mean(options),2),
    meanAttempts = round(mean(attempts),2),
    FirstAcc = round(mean(FAcorrect),2),
    LastAcc = round(mean(LAcorrect),2),
    zFirstAcc = round(zbin(round(mean(FAcorrect),2),round(mean(options),2)),2),
    zLastAcc = round(zbin(round(mean(LAcorrect),2),round(mean(options),2)),2)
    #trialDifficulty = unique(trialDifficulty),
    #tokenDifficulty = unique(tokenDifficulty)
  )

##################################################################################################################

# # write processed data to csvs
# # write.csv(cdr_all_userType, paste0("processed_cdrData_all_userType_",dataDate,".csv"),row.names = F)
# write.csv(cdrData_autoGenerate, paste0("processed_cdrData_systemCalls_",dataDate,".csv"),row.names = F)
# write.csv(cdrData_userCalls, paste0("processed_cdrData_userCalls_",dataDate,".csv"),row.names = F)
# write.csv(interactionsData, paste0("processed_interactionsData_",dataDate,".csv"),row.names = F)
# #write.csv(interactionsData_lessons, paste0("processed_interactionsData_lessons_",dataDate,".csv"),row.names = F)
# write.csv(UASdata, paste0("processed_UASdata_",dataDate,".csv"),row.names = F)

