##############################################
#### before you run this code, read below ####

# under the heading "run the latest data!" below, update the file to the latest. You can do this by:
# 1. sign into metabase
# 2. click on the Activity button in the top right
# 3. click on "user data export"
# 4. It should auto-refresh the query. Check this by confirming the date in the field "UAS: Created At" is current.
# 5. If it is ok, click the down arrow on the right side of the screen and choose .csv. If it is not, click the refresh button and then download.
# 6. Find the downloaded file and append "UAS_" to the front of the file. Move this to the appropriate working directory.
# 7. Click on the Activity button again, and choose "cdr-all calls"
# 8. Confirm the auto-refresh query, and download the results as a .csv.
# 9. Find the downloaded file and append "CDR_" to the front of the file. Move this to the appropriate working directory.

#You will also need the csvs "vlookupStudentStudyId_international.csv" and "vlookupStudentStudyId_local.csv" for the code to run properly.
#These will not be shared on github for privacy reasons. Contact me for them if you need them.

###############
#### setup ####

library(tidyverse)
#library(dplyr)
library(anytime)
library(jsonlite)
library(DT)
library(chron)
library(lubridate)
library(openxlsx)
#library(lme4)
#library(lmerTest)
#library(agricolae)
#library(bsselectR)

#data cannot go on github due to sensitive user information
#store it somewhere else local on your machine and call it
#mac environment
setwd("~/Documents/IvoryCoast/data/")

# run the latest data!
CIVdata = read.csv(file = "UAS_query_result_2018-12-05T15_31_49.056Z.csv")
cdrData = read.csv(file = "CDR_query_result_2018-11-19T16_40_56.492Z.csv")

#merge in student study id
#in Michel's list
vlookupStudentStudyId_international = read.csv("vlookupStudentStudyId_international.csv",header=TRUE)
colnames(vlookupStudentStudyId_international)[1] <- "users.mobile_number"
vlookupStudentStudyId_international$users.mobile_number = as.factor(vlookupStudentStudyId_international$users.mobile_number)

suppressWarnings(CIVdata <- CIVdata %>%
                   left_join(vlookupStudentStudyId_international, by = c("users.mobile_number")))

#create filter for study ID binary (will be filter later)
#convert current unit to binary
##There are duplicate values but there is not a simple fix for the pilot data. Part of the fix below.
CIVdata = CIVdata %>%
  mutate(
    inMichelsList = ifelse(is.na(CIVdata$studentStudyId), 0, 1)
  )

#merge in IPA
#cmsToken_table <- read.xlsx('cms_tokens_2018-11-24.xlsx', 'tokens')
#cmsToken_table = cmsToken_table %>%
#  select(id, phonetics_auditory, spelling_visual, syllable_structure, token_type_id)

#for token a
# colnames(cmsToken_table)[1] <- "cmsQuestions.token_a_id"
# colnames(cmsToken_table)[2] <- "phonetics_auditory_token_a"
# colnames(cmsToken_table)[3] <- "spelling_visual_token_a"
# colnames(cmsToken_table)[4] <- "syllable_structure_token_a"
# colnames(cmsToken_table)[5] <- "token_type_id_token_a"
# suppressWarnings(CIVdata <- CIVdata %>%
#                    left_join(cmsToken_table, by = c("cmsQuestions.token_a_id")))
# 
# #for token b
# colnames(cmsToken_table)[1] <- "cmsQuestions.token_b_id"
# colnames(cmsToken_table)[2] <- "phonetics_auditory_token_b"
# colnames(cmsToken_table)[3] <- "spelling_visual_token_b"
# colnames(cmsToken_table)[4] <- "syllable_structure_token_b"
# colnames(cmsToken_table)[5] <- "token_type_id_token_b"
# suppressWarnings(CIVdata <- CIVdata %>%
#                    left_join(cmsToken_table, by = c("cmsQuestions.token_b_id")))
# 
# #for token c
# colnames(cmsToken_table)[1] <- "cmsQuestions.token_c_id"
# colnames(cmsToken_table)[2] <- "phonetics_auditory_token_c"
# colnames(cmsToken_table)[3] <- "spelling_visual_token_c"
# colnames(cmsToken_table)[4] <- "syllable_structure_token_c"
# colnames(cmsToken_table)[5] <- "token_type_id_token_c"
# suppressWarnings(CIVdata <- CIVdata %>%
#                    left_join(cmsToken_table, by = c("cmsQuestions.token_c_id")))

#extract date from weird metabase timestamp
CIVdata$date <- as.Date(substr(CIVdata$UAS.created_at, 1, 10))

#extract time from weird metabase timestamp
CIVdata$time <- substr(CIVdata$UAS.created_at, 12, 19)

#create DateTime from merging extracted date and time
CIVdata$dateTime <- strptime(paste(CIVdata$date, " ", CIVdata$time), "%Y-%m-%d %H:%M:%S")
CIVdata$dateTime <- as.POSIXct(CIVdata$dateTime)

#intervention date range
CIVdata$interventionDateRange <- ifelse(CIVdata$date >= "2018-10-24", 1, 0)

#extract the raw hour of the timestamp
suppressWarnings(CIVdata$hourExtract <- hour(hms(as.character(CIVdata$time))))
CIVdata$timeUsersPreferred = ifelse(((CIVdata$hourExtract == 17) |
                                       (CIVdata$hourExtract == 18) |
                                       (CIVdata$hourExtract == 19)), 1, 0)

#change question text to char
CIVdata$cmsQuestions.question_text = as.character(CIVdata$cmsQuestions.question_text)

#sort by studentStudyId and DateTime
CIVdata = dplyr::arrange(CIVdata, studentStudyId, dateTime)

###############################################################################################################
#### create CIVdata_filter and take out all non-study IDs and dates, also exclude student 30 (the teacher) ####
CIVdata_filter = CIVdata %>%
  filter(interventionDateRange == 1, inMichelsList == 1, studentStudyId != 30, studentStudyId != 38, studentStudyId != 1)

#extract elapsed time (sec) per lesson since last line

CIVdata_filter = CIVdata_filter %>%
  mutate(changeLesson = ifelse((UAS.lesson_id != lag(UAS.lesson_id)),1,ifelse(date == lag(date) &
                                                                             hourExtract - lag(hourExtract) < 2,0,1)),
         elapsedTimeSec = ifelse((UAS.lesson_id == lag(UAS.lesson_id) &
                                    date == lag(date) &
                                    hourExtract - lag(hourExtract) < 2), dateTime - lag(dateTime), NA))

#flag when a new question happens
CIVdata_filter = CIVdata_filter %>%
  mutate(changeQuestion = ifelse(changeLesson == 1, 1, ifelse(cmsQuestions.question_text != lag(cmsQuestions.question_text), 1, 0)),
         changeAttempt = ifelse(is.na(elapsedTimeSec), 0, ifelse(changeLesson == 1, 0, ifelse(changeQuestion == 1, 0, 1)))) %>%
  ungroup %>%
  group_by(studentStudyId) 

#fix the first row for these columns due to lag()
CIVdata_filter[1, c("changeQuestion")] <- 1
CIVdata_filter[1, c("changeAttempt")] <- 0

#use cumsum to number questions (per unit and overall) and attempts
CIVdata_filter = CIVdata_filter %>%
  mutate(questionNumberOverall = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(studentStudyId, UAS.unit_id) %>%
  mutate(questionNumberPerUnit = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(studentStudyId, UAS.unit_id, questionNumberOverall) %>%
  mutate(attemptNumber = cumsum(changeAttempt),
         attemptNumber = attemptNumber + 1)

CIVdata_filter[1, c("changeLesson")] <- 1
CIVdata_filter[1, c("elapsedTimeSec")] <- NA

#use cumsum to number the lessons overall
CIVdata_filter = CIVdata_filter %>%
  ungroup() %>%
  group_by(studentStudyId) %>%
  mutate(lessonNumberOverall = cumsum(changeLesson)) %>%
  ungroup() %>%
  group_by(studentStudyId,UAS.unit_id) %>%
  mutate(lessonNumberPerUnit = cumsum(changeLesson)) %>%
  ungroup() %>%
  group_by(studentStudyId, lessonNumberOverall) %>%
  mutate(questionNumberPerLesson = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(studentStudyId, cmsQuestions.trial_id) %>%
  mutate(questionNumberPerTrialId = cumsum(changeQuestion))


#make V02 of co-occurring distractor token id (fix issue of "[" and "]" appearing only for units 3 and 4)
#create IPA for co-occurring distractors
#create visual spelling of co-occurring distractors
CIVdata_filter = CIVdata_filter %>%
  mutate(cmsQuestions.distractor_tokens_V02 = ifelse(is.na(cmsQuestions.token_c_id), "null", paste(cmsQuestions.token_b_id,", ",cmsQuestions.token_c_id, sep = "")),
         cmsQuestions.distractor_tokens_IPA = ifelse(is.na(cmsQuestions.token_c_id), "null", paste(phonetics_auditory_token_b,", ",phonetics_auditory_token_c, sep = "")),
         cmsQuestions.distractor_tokens_spelling = ifelse(is.na(cmsQuestions.token_c_id), "null", paste(spelling_visual_token_b,", ",spelling_visual_token_c, sep = "")))

CIVdata_filter$usersToUnits.currentUnit = as.character(CIVdata_filter$usersToUnits.current)
CIVdata_filter$usersToUnits.current = NULL


#merge the promotionType file
CIVdata_filter = CIVdata_filter %>%
  mutate(userUnit = paste(studentStudyId,"_",UAS.unit_id,sep=""))

CIVdata_filter = CIVdata_filter %>%
  mutate(unitTrial = paste(UAS.unit_id,"_",cmsQuestions.trial_id,sep=""))

CIVdata_filter$unitTrial = as.factor(CIVdata_filter$unitTrial)

setwd("~/Documents/GitHub/IvoryCoast_IVR_Reports/TRECC Analyses/")

vlookupPromotionType = read.csv("promotionType.csv",header=TRUE)
vlookupPromotionType$userUnit = as.factor(vlookupPromotionType$userUnit)
suppressWarnings(CIVdata_filter <- CIVdata_filter %>%
                   left_join(vlookupPromotionType, by = c("userUnit")))

######################################################################################################
#### Summarize multiple-attempts down into one row with First-attempt and Last-attempt accuracies ####
setwd("~/Documents/IvoryCoast/data/")

# First attempt statistics
CIVdata_filter.firstattempt <- CIVdata_filter[CIVdata_filter$attemptNumber==1,] %>%
  group_by(studentStudyId,cmsQuestions.id) %>%
  summarise(
    FAcorrect = max(UAS.correct)
  ) 
# Last attempt statistics
CIVdata_filter.lastattempt <- CIVdata_filter %>%
  group_by(studentStudyId,cmsQuestions.id) %>%
  summarise(
    LAcorrect = max(UAS.correct),
    attempts = max(attemptNumber)
  )
# Merge first and last attempt data
CIVdata_filter.questionid <- merge(CIVdata_filter.firstattempt,CIVdata_filter.lastattempt,by=c('studentStudyId','cmsQuestions.id'))

# Record the number of answer options for each question (Y/N=2, A/B/C=3)
CIVdata_filter$options = 3
two_choice = is.na(CIVdata_filter$cmsQuestions.token_c_id)
CIVdata_filter[two_choice,]$options = 2

# This function will be useful later for comparing 2-option and 3-option questions on equal footing
zbin <- function(acc,n_options){
  dist_mean = 1/n_options
  dist_sd = dist_mean * (1-dist_mean)
  zscr <- (acc-dist_mean)/dist_sd
  return(zscr)
}

# Descriptives for each question ID
CIVdata_filter.descriptives <- CIVdata_filter %>%
  group_by(studentStudyId,cmsQuestions.id) %>%
  summarise(
    trialID = unique(cmsQuestions.trial_id),
    token_a_ID = unique(cmsQuestions.token_id),
    token_a_IPA = unique(phonetics_auditory_token_a),
    token_a_spelling = unique(spelling_visual_token_a),
    token_a_syllable_structure = unique(syllable_structure_token_a),
    options = mean(options),
    token_a_type = unique(token_type_id_token_a),
    trialDifficulty = mean(cmsQuestions.difficulty_level_trial),
    tokenDifficulty = mean(cmsQuestions.difficulty_level_token)
  )
CIVdata_filter.questionid <- merge(CIVdata_filter.questionid,CIVdata_filter.descriptives,by=c('studentStudyId','cmsQuestions.id'))
CIVdata_filter.questionid$FAz = zbin(CIVdata_filter.questionid$FAcorrect,CIVdata_filter.questionid$options)
CIVdata_filter.questionid$LAz = zbin(CIVdata_filter.questionid$LAcorrect,CIVdata_filter.questionid$options)

######################################################################################################
#### Summarize by Token A ID ####
# This analysis excludes all of the Y/N (two-option) questions because students were not given multiple
# attempts with these questions. Getting only one attempt skews the accuracy ratings
CIVdata_filter.tokenid <- CIVdata_filter.questionid[CIVdata_filter.questionid$options>2,] %>%
  group_by(token_a_ID, token_a_IPA, token_a_spelling, token_a_syllable_structure, token_a_type) %>%
  summarise(
    students = n_distinct(studentStudyId),
    presentations = n(),
    meanAttempts = round(mean(attempts),2),
    FirstAcc = round(mean(FAcorrect),2),
    LastAcc = round(mean(LAcorrect),2),
    zFirstAcc = round(zbin(mean(FAcorrect),mean(options)),2),
    zLastAcc = round(zbin(mean(LAcorrect),mean(options)),2),
    trialDifficulty = round(mean(trialDifficulty),2),
    tokenDifficulty = unique(tokenDifficulty)
  )

#CIVdata_filter.tokenid.ALLtokens <- merge(cmsToken_table[,c('id','phonetics_auditory','syllable_structure')],CIVdata_filter.tokenid,by.y='tokenID',by.x='id',all.x=TRUE)
#CIVdata_filter.tokenid <- merge(cmsToken_table[,c('id','phonetics_auditory','syllable_structure')],CIVdata_filter.tokenid,by.y='tokenID',by.x='id')

# Summarize by Trial (Question-Template) ID
CIVdata_filter.trialid <- CIVdata_filter.questionid %>%
  group_by(trialID) %>%
  summarise(
    students = n_distinct(studentStudyId),
    presentations = n(),
    options = round(mean(options),2),
    meanAttempts = round(mean(attempts),2),
    FirstAcc = round(mean(FAcorrect),2),
    LastAcc = round(mean(LAcorrect),2),
    zFirstAcc = zbin(round(mean(FAcorrect),2),round(mean(options),2)),
    zLastAcc = zbin(round(mean(LAcorrect),2),round(mean(options),2)),
    trialDifficulty = round(mean(trialDifficulty),2),
    tokenDifficulty = round(mean(tokenDifficulty),2)
  )


##################################################################################################################


###################
#### CDR table ####

#create date and intervention date range
cdrData$date <- as.Date(substr(cdrData$calldate, 1, 10))
cdrData$interventionDateRange <- ifelse(cdrData$date >= "2018-10-24", 1, 0)

#filter for dates within the date range only
cdrData = cdrData %>%
  filter(interventionDateRange == "1")

cdrData$internationalNumber = sub('.*,\\s*','', cdrData$lastdata)

vlookupStudentStudyId_international = read.csv("vlookupStudentStudyId_international.csv",header=TRUE)
colnames(vlookupStudentStudyId_international)[1] <- "internationalNumber"
colnames(vlookupStudentStudyId_international)[2] <- "studentStudyId_international"
vlookupStudentStudyId_international$internationalNumber = as.factor(vlookupStudentStudyId_international$internationalNumber)
suppressWarnings(cdrData <- cdrData %>%
                   left_join(vlookupStudentStudyId_international, by = c("internationalNumber")))

vlookupStudentStudyId_local = read.csv("vlookupStudentStudyId_local.csv",header=TRUE, colClasses = c("character","numeric"))
colnames(vlookupStudentStudyId_local)[1] <- "src"
vlookupStudentStudyId_local$src = as.factor(vlookupStudentStudyId_local$src)
suppressWarnings(cdrData <- cdrData %>%
                   left_join(vlookupStudentStudyId_local, by = c("src")))

cdrData = cdrData %>%
  mutate(studentStudyId = ifelse(!is.na(studentStudyId_international),studentStudyId_international, ifelse(!is.na(studentStudyId_local),studentStudyId_local,NA)),
         inMichelsList = ifelse(is.na(studentStudyId), 0, 1))

#extract date from weird metabase timestamp
cdrData$date <- as.Date(substr(cdrData$calldate, 1, 10))

#extract time from weird metabase timestamp
cdrData$time <- substr(cdrData$calldate, 12, 19)

#create DateTime from merging extracted date and time
cdrData$dateTime <- strptime(paste(cdrData$date, " ", cdrData$time), "%Y-%m-%d %H:%M:%S")
cdrData$dateTime <- as.POSIXct(cdrData$dateTime)

#hour extract
suppressWarnings(cdrData$hourExtract <- hour(as.character(cdrData$calldate)))
cdrData$timeUsersPreferred = ifelse(((cdrData$hourExtract == 17) |
                                              (cdrData$hourExtract == 18) |
                                              (cdrData$hourExtract == 19)), 1, 0)

# cdrData = cdrData %>%
#   #mutate(extraLine = ifelse(studentStudyId == lead(studentStudyId) & callstart == lead(callstart), 1, 0)) %>%
#   filter(
#          #extraLine == 0,
#          #inMichelsList == 1,
#          studentStudyId != 30,
#          studentStudyId != 1,
#          studentStudyId != 38
#          )

#cdrData = dplyr::arrange(cdrData, studentStudyId, date, hourExtract, calldate)

#Case 1: User initate the call
#Case 2: IVR callbacks to the User
cdrData = cdrData %>%
  mutate(userInitiatesCall = ifelse(dst == "22521323577", 1, 0),
         IVRCallsBack = ifelse(dst == "menu", 1, 0))

#callNumbering user call initiation
cdrData_filter_userInitiates = cdrData %>%
  ungroup() %>%
  filter(userInitiatesCall == 1) %>%
  mutate(shortCall = ifelse(billsec<30,"1","0")) %>%
  #filter(shortCall == "0") %>%
  group_by(studentStudyId) %>%
  mutate(callNumber = cumsum(inMichelsList))

#callNumering
cdrData_filter_IVRCall = cdrData %>%
  ungroup() %>%
  filter(IVRCallsBack == 1) %>%
  mutate(shortCall = ifelse(billsec<30,"1","0")) %>%
  #filter(shortCall == "0") %>%
  group_by(studentStudyId) %>%
  mutate(callNumber = cumsum(inMichelsList))

