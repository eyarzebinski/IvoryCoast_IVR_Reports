##############################################
#### before you run this code, read below ####

#TODO update the procedures from metabase to latest server.
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

######################################################################################
#### setup ####

library(tidyverse)
library(anytime)
library(jsonlite)
library(DT)
library(chron)
library(lubridate)
#library(openxlsx)
#library(lme4)
#library(lmerTest)
#library(agricolae)
#library(bsselectR)

#data cannot go on github due to sensitive user information
#store it somewhere else local on your machine and call it

#mac environment
setwd("/Volumes/OganData/Ivory Coast/201901 Study")

#set file date
date = 20190215

# run the latest data!
# run UAS.sql on database; export it as SEMICOLON separated csv called 'UAS_YYYYMMDD.csv'
UASdata = read.csv(file = paste0("UAS_",date,".csv"), sep = ";")

# run CDR.sql on database; export it as SEMICOLON separated csv called 'CDR_YYYYMMDD.csv'
cdrData = read.csv(file = paste0("CDR_",date,".csv"), sep = ";")

#run INTERACTIONS.sql on databse; export it as SEMICOLON separated csv called 'interactions_YYYYMMDD.csv'
interactionsData = read.csv(file = paste0("interactions_",date,".csv"), sep = ";")

######################################################################################


##################################
#### BASIC PROCESSING FOR UAS ####

#First: pull the latest TRECC_PHASE2_PHONE sheet, save it.

#merge in student study id
#in Michel's list
#open the TRECC PHASE 2 google doc and confirm the last edit date. Export if there are new edits.
vlookupStudentStudyId_local = read.csv("phoneNumberMapping_20190213.csv",header=TRUE)
colnames(vlookupStudentStudyId_local)[1] <- "sessions.mobile_number"
#vlookupStudentStudyId_local$users.mobile_number = as.factor(vlookupStudentStudyId_local$users.mobile_number)

suppressWarnings(UASdata <- UASdata %>%
                   left_join(vlookupStudentStudyId_local, by = c("sessions.mobile_number")))

#create filter for study ID binary (will be filter later)
#convert current unit to binary
##There are duplicate values but there is not a simple fix for the pilot data. Part of the fix below.
UASdata = UASdata %>%
  mutate(inMichelsList = ifelse(is.na(UASdata$studentStudyId), 0, 1))

#rename
#UASdata$studentStudyID = UASdata$ID.Portable
#UASdata$ID.Portable = NULL

#extract date from weird timestamp format
UASdata$date <- as.Date(substr(UASdata$UAS.created_at, 1, 10))

#extract time from weird timestamp format
UASdata$time <- substr(UASdata$UAS.created_at, 12, 19)

#create DateTime from merging extracted date and time
UASdata$dateTime <- strptime(paste(UASdata$date, " ", UASdata$time), "%Y-%m-%d %H:%M:%S")
UASdata$dateTime <- as.POSIXct(UASdata$dateTime)

#intervention date range
UASdata$interventionDateRange <- ifelse(UASdata$date >= "2019-02-09", 1, 0)

#extract the raw hour of the timestamp
suppressWarnings(UASdata$hourExtract <- hour(hms(as.character(UASdata$time))))
suppressWarnings(UASdata$minuteExtract <- minute(hms(as.character(UASdata$time))))

#change question text to char
UASdata$cmsQuestions.question_text = as.character(UASdata$cmsQuestions.question_text)

#sort by studentStudyId and DateTime
UASdata = dplyr::arrange(UASdata, studentStudyId, dateTime)

###########################################
#### BASIC PROCESSING FOR INTERACTIONS ####

#sort by studentStudyId and DateTime
interactionsData  = interactionsData %>%
  mutate(date = as.POSIXct(substr(interactions.created_at,1,10)))

interactionsData = dplyr::arrange(interactionsData, interactions.user_id, interactionData.id)

#change all NULL values to NA in particular columns
interactionsData$cdr.uniqueId[interactionsData$cdr.uniqueId == "NULL"] <- NA
interactionsData$UAS.lessonId[interactionsData$UAS.lessonId == "NULL"] <- NA
interactionsData$UAS.questionId[interactionsData$UAS.questionId == "NULL"] <- NA

#now fill the NA uniqueId values with the last real value
interactionsData = interactionsData %>%
  fill(cdr.uniqueId, .direction = "down")

#create a table of uniqueIDs and lessonIDs for a later join
interactionsData_lessons = interactionsData %>%
  group_by(interactions.user_id, cdr.uniqueId, UAS.lessonId) %>%
  summarize(count = n()) %>%
  mutate(CONCAT_lesson_treccStudyID = paste0(UAS.lessonId,interactions.user_id))

#identify and delete lines that don't have a lessonID
interactionsData_lessons$UAS.lessonId = as.character(interactionsData_lessons$UAS.lessonId)
interactionsData_lessons[c("UAS.lessonId")][is.na(interactionsData_lessons[c("UAS.lessonId")])] <- "delete"
interactionsData_lessons = interactionsData_lessons[!interactionsData_lessons$UAS.lessonId == "delete", ]

#reorder to put the concat first
interactionsData_lessons = interactionsData_lessons[,c(which(colnames(interactionsData_lessons)=="CONCAT_lesson_treccStudyID"),which(colnames(interactionsData_lessons)!="CONCAT_lesson_treccStudyID"))]

#drop unneeded columns
interactionsData_lessons$interactions.user_id = NULL
interactionsData_lessons$UAS.lessonId = NULL
interactionsData_lessons$count = NULL

#get just ONE for each concat
interactionsData_lessons = interactionsData_lessons %>%
  group_by(CONCAT_lesson_treccStudyID, cdr.uniqueId) %>%
  summarize(count = n())

#now use this to join the unique ID into the UAS! (but catch up on the UAS processing first...)

########################################################################
#### create UASdata_filter and take out all non-study IDs and dates ####
UASdata_filter = UASdata %>%
  filter(interventionDateRange == 1, inMichelsList == 1)

#add in lesson & trecc ID concatentation (note: this is not yet the study ID - this is the ID trecc assigns in the order a unique phone called in)
UASdata_filter$CONCAT_lesson_treccStudyID = paste0(UASdata_filter$UAS.lesson_id,UASdata_filter$sessions.user_id)

#join interactionsData_lessons into UAS
suppressWarnings(UASdata_filter <- UASdata_filter %>%
                   left_join(interactionsData_lessons, by = c("CONCAT_lesson_treccStudyID")))

#TODO confirm there are not dupes adding extra lines after this point!


##################################
#### BASIC PROCESSING FOR CDR ####

#extract date from weird metabase timestamp
cdrData$date <- as.Date(substr(cdrData$calldate, 1, 10))

#extract time from weird metabase timestamp
cdrData$time <- substr(cdrData$calldate, 12, 19)

#create DateTime from merging extracted date and time
cdrData$dateTime <- strptime(paste(cdrData$date, " ", cdrData$time), "%Y-%m-%d %H:%M:%S")
cdrData$dateTime <- as.POSIXct(cdrData$dateTime)

#hour extract
suppressWarnings(cdrData$hourExtract <- hour(as.character(cdrData$dateTime)))
suppressWarnings(cdrData$minuteExtract <- minute(as.character(cdrData$dateTime)))

#join the demographic information
cdrData = cdrData %>%
  mutate(localPhoneNumber = src)

suppressWarnings(cdrData <- cdrData %>%
                   left_join(vlookupStudentStudyId_local, by = c("localPhoneNumber")))

#split file into the following 2 cases, ignore all calls that don't match this criteria:

#Case 1: User initate the call
#src = User phone number
#dst = 22012349
cdrData_userCalls = cdrData %>%
  filter(!is.na(studentStudyId))

#Case 2: IVR callbacks to the User
#src = 22012349
#dst = User phone number
cdrData_systemCalls = cdrData %>%
  filter(is.na(studentStudyId) & src == "22012349") %>%
  select(-c(localPhoneNumber, sessions.mobile_number, studentStudyId, village, school)) %>%
  mutate(localPhoneNumber = dst)

suppressWarnings(cdrData_systemCalls <- cdrData_systemCalls %>%
                   left_join(vlookupStudentStudyId_local, by = c("localPhoneNumber")))

#sort by ID.Portable and dateTime for call numbering
cdrData_userCalls = dplyr::arrange(cdrData_userCalls, studentStudyId, dateTime)

#number calls in cdrData_userCalls
cdrData_userCalls = cdrData_userCalls %>%
  mutate(newCall = ifelse(lag(uniqueid) != uniqueid, 1, 0))

cdrData_userCalls[1, c("newCall")] <- 1

cdrData_userCalls = cdrData_userCalls %>%
  group_by(studentStudyId) %>%
  mutate(callNumber = cumsum(newCall))

#### now join in the uniqueId found in UAS to show in CDR which calls have UAS data
interactionsData_lessons$CONCAT_lesson_treccStudyID = NULL
colnames(interactionsData_lessons)[1] <- "uniqueid"
colnames(interactionsData_lessons)[2] <- "callHasUASData"

interactionsData_lessons$uniqueid = as.character(interactionsData_lessons$uniqueid)
cdrData_userCalls$uniqueid = as.character(cdrData_userCalls$uniqueid)

suppressWarnings(cdrData_userCalls <- cdrData_userCalls %>%
                   left_join(interactionsData_lessons, by = c("uniqueid")))

#fill in callHasUASData NAs with 0
cdrData_userCalls[c("callHasUASData")][is.na(cdrData_userCalls[c("callHasUASData")])] <- 0

#################################################
#### secondary processing in UAS data filter ####

UASdata_filter$cmsQuestions.distractor_tokens[UASdata_filter$cmsQuestions.distractor_tokens == "NULL"] <- NA
UASdata_filter$cmsQuestions.text_output_structure[UASdata_filter$cmsQuestions.text_output_structure == "NULL"] <- NA
UASdata_filter$cmsQuestions.token_c_id[UASdata_filter$cmsQuestions.token_c_id == "NULL"] <- NA
UASdata_filter$cmsTokens_C.token_id_c[UASdata_filter$cmsTokens_C.token_id_c == "NULL"] <- NA
UASdata_filter$language_id_token_c[UASdata_filter$language_id_token_c == "NULL"] <- NA
UASdata_filter$open_closed_token_c[UASdata_filter$open_closed_token_c == "NULL"] <- NA
UASdata_filter$phonetics_auditory_token_c[UASdata_filter$phonetics_auditory_token_c == "NULL"] <- NA
UASdata_filter$simple_complex_token_c[UASdata_filter$simple_complex_token_c == "NULL"] <- NA
UASdata_filter$soundfile_name_token_c[UASdata_filter$soundfile_name_token_c == "NULL"] <- NA
UASdata_filter$spelling_visual_token_c[UASdata_filter$spelling_visual_token_c == "NULL"] <- NA
UASdata_filter$syllable_structure_token_c[UASdata_filter$syllable_structure_token_c == "NULL"] <- NA
UASdata_filter$token_type_id_token_c[UASdata_filter$token_type_id_token_c == "NULL"] <- NA
UASdata_filter$userUnitStats.user_id[UASdata_filter$userUnitStats.user_id == "NULL"] <- NA
UASdata_filter$userUnitStats.unit_id[UASdata_filter$userUnitStats.unit_id == "NULL"] <- NA
UASdata_filter$userUnitStats.questions_attempted[UASdata_filter$userUnitStats.questions_attempted == "NULL"] <- NA
UASdata_filter$userUnitStats.average_user_mastery_score[UASdata_filter$userUnitStats.average_user_mastery_score == "NULL"] <- NA
UASdata_filter$userUnitStats.completed[UASdata_filter$userUnitStats.completed == "NULL"] <- NA

#extract elapsed time (sec) per lesson since last line
UASdata_filter = UASdata_filter %>%
  mutate(changeLesson = ifelse((UAS.lesson_id != lag(UAS.lesson_id)),1,ifelse(date == lag(date) &
                                                                                hourExtract - lag(hourExtract) < 2,0,1)),
         elapsedTimeSec = ifelse((UAS.lesson_id == lag(UAS.lesson_id) &
                                    date == lag(date) &
                                    hourExtract - lag(hourExtract) < 2), dateTime - lag(dateTime), NA))

#flag when a new question happens
UASdata_filter = UASdata_filter %>%
  mutate(changeQuestion = ifelse(changeLesson == 1, 1, ifelse(cmsQuestions.question_text != lag(cmsQuestions.question_text), 1, 0)),
         changeAttempt = ifelse(is.na(elapsedTimeSec), 0, ifelse(changeLesson == 1, 0, ifelse(changeQuestion == 1, 0, 1)))) %>%
  ungroup %>%
  group_by(studentStudyId)

#fix the first row for these columns due to lag()
UASdata_filter[1, c("changeQuestion")] <- 1
UASdata_filter[1, c("changeAttempt")] <- 0

#use cumsum to number questions (per unit and overall) and attempts
UASdata_filter = UASdata_filter %>%
  mutate(questionNumberOverall = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(studentStudyId, UAS.unit_id) %>%
  mutate(questionNumberPerUnit = cumsum(changeQuestion)) %>%
  ungroup() %>%
  group_by(studentStudyId, UAS.unit_id, questionNumberOverall) %>%
  mutate(attemptNumber = cumsum(changeAttempt),
         attemptNumber = attemptNumber + 1)

UASdata_filter[1, c("changeLesson")] <- 1
UASdata_filter[1, c("elapsedTimeSec")] <- NA

#use cumsum to number the lessons overall
UASdata_filter = UASdata_filter %>%
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
UASdata_filter = UASdata_filter %>%
  mutate(cmsQuestions.distractor_tokens_V02 = ifelse(is.na(cmsQuestions.token_c_id), "null", paste(cmsQuestions.token_b_id,", ",cmsQuestions.token_c_id, sep = "")),
         cmsQuestions.distractor_tokens_IPA = ifelse(is.na(cmsQuestions.token_c_id), "null", paste(phonetics_auditory_token_b,", ",phonetics_auditory_token_c, sep = "")),
         cmsQuestions.distractor_tokens_spelling = ifelse(is.na(cmsQuestions.token_c_id), "null", paste(spelling_visual_token_b,", ",spelling_visual_token_c, sep = "")))

UASdata_filter$usersToUnits.currentUnit = as.character(UASdata_filter$usersToUnits.current)
UASdata_filter$usersToUnits.current = NULL




######################################################################################################
#### Summarize multiple-attempts down into one row with First-attempt and Last-attempt accuracies ####
setwd("~/Documents/IvoryCoast/data/")

# First attempt statistics
UASdata_filter.firstattempt <- UASdata_filter[UASdata_filter$attemptNumber==1,] %>%
  group_by(studentStudyId,cmsQuestions.id) %>%
  summarise(
    FAcorrect = max(UAS.correct)
  ) 
# Last attempt statistics
UASdata_filter.lastattempt <- UASdata_filter %>%
  group_by(studentStudyId,cmsQuestions.id) %>%
  summarise(
    LAcorrect = max(UAS.correct),
    attempts = max(attemptNumber)
  )
# Merge first and last attempt data
UASdata_filter.questionid <- merge(UASdata_filter.firstattempt,UASdata_filter.lastattempt,by=c('studentStudyId','cmsQuestions.id'))

# Record the number of answer options for each question (Y/N=2, A/B/C=3)
UASdata_filter$options = 3
two_choice = is.na(UASdata_filter$cmsQuestions.token_c_id)
UASdata_filter[two_choice,]$options = 2

# This function will be useful later for comparing 2-option and 3-option questions on equal footing
zbin <- function(acc,n_options){
  dist_mean = 1/n_options
  dist_sd = dist_mean * (1-dist_mean)
  zscr <- (acc-dist_mean)/dist_sd
  return(zscr)
}

# Descriptives for each question ID
UASdata_filter.descriptives <- UASdata_filter %>%
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
UASdata_filter.questionid <- merge(UASdata_filter.questionid,UASdata_filter.descriptives,by=c('studentStudyId','cmsQuestions.id'))
UASdata_filter.questionid$FAz = zbin(UASdata_filter.questionid$FAcorrect,UASdata_filter.questionid$options)
UASdata_filter.questionid$LAz = zbin(UASdata_filter.questionid$LAcorrect,UASdata_filter.questionid$options)

######################################################################################################
#### Summarize by Token A ID ####
# This analysis excludes all of the Y/N (two-option) questions because students were not given multiple
# attempts with these questions. Getting only one attempt skews the accuracy ratings
UASdata_filter.tokenid <- UASdata_filter.questionid[UASdata_filter.questionid$options>2,] %>%
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

#UASdata_filter.tokenid.ALLtokens <- merge(cmsToken_table[,c('id','phonetics_auditory','syllable_structure')],UASdata_filter.tokenid,by.y='tokenID',by.x='id',all.x=TRUE)
#UASdata_filter.tokenid <- merge(cmsToken_table[,c('id','phonetics_auditory','syllable_structure')],UASdata_filter.tokenid,by.y='tokenID',by.x='id')

# Summarize by Trial (Question-Template) ID
UASdata_filter.trialid <- UASdata_filter.questionid %>%
  group_by(trialID) %>%
  summarise(
    students = n_distinct(studentStudyId),
    presentations = n(),
    options = round(mean(options),2),
    meanAttempts = round(mean(attempts),2),
    FirstAcc = round(mean(FAcorrect),2),
    LastAcc = round(mean(LAcorrect),2),
    zFirstAcc = round(zbin(round(mean(FAcorrect),2),round(mean(options),2)),2),
    zLastAcc = round(zbin(round(mean(LAcorrect),2),round(mean(options),2)),2),
    trialDifficulty = round(mean(trialDifficulty),2),
    tokenDifficulty = round(mean(tokenDifficulty),2)
  )


##################################################################################################################


# ###################
# #### CDR table ####
# 
# #create date and intervention date range
# cdrData$date <- as.Date(substr(cdrData$calldate, 1, 10))
# cdrData$interventionDateRange <- ifelse(cdrData$date >= "2018-02-01", 1, 0)
# 
# #filter for dates within the date range only
# cdrData = cdrData %>%
#   filter(interventionDateRange == "1")
# 
# cdrData$internationalNumber = sub('.*,\\s*','', cdrData$lastdata)
# 
# vlookupStudentStudyId_international = read.csv("vlookupStudentStudyId_fullStudy_international.csv",header=TRUE)
# colnames(vlookupStudentStudyId_international)[1] <- "internationalNumber"
# colnames(vlookupStudentStudyId_international)[2] <- "studentStudyId_international"
# vlookupStudentStudyId_international$internationalNumber = as.factor(vlookupStudentStudyId_international$internationalNumber)
# suppressWarnings(cdrData <- cdrData %>%
#                    left_join(vlookupStudentStudyId_international, by = c("internationalNumber")))
# 
# vlookupStudentStudyId_local = read.csv("vlookupStudentStudyId_fullStudy_local.csv",header=TRUE, colClasses = c("character","numeric"))
# colnames(vlookupStudentStudyId_local)[1] <- "src"
# vlookupStudentStudyId_local$src = as.numeric(vlookupStudentStudyId_local$src)
# suppressWarnings(cdrData <- cdrData %>%
#                    left_join(vlookupStudentStudyId_local, by = c("src")))
# 
# cdrData = cdrData %>%
#   mutate(studentStudyId = ifelse(!is.na(studentStudyId_international),studentStudyId_international, ifelse(!is.na(studentStudyId_local),studentStudyId_local,NA)),
#          inMichelsList = ifelse(is.na(studentStudyId), 0, 1))
# 
# 
# 
# 
# #callNumbering user call initiation
# cdrData_filter_userInitiates = cdrData %>%
#   ungroup() %>%
#   filter(userInitiatesCall == 1) %>%
#   mutate(shortCall = ifelse(billsec<30,"1","0")) %>%
#   #filter(shortCall == "0") %>%
#   group_by(studentStudyId) %>%
#   mutate(callNumber = cumsum(inMichelsList))
# 
# #callNumbering
# cdrData_filter_IVRCall = cdrData %>%
#   ungroup() %>%
#   filter(IVRCallsBack == 1) %>%
#   mutate(shortCall = ifelse(billsec<30,"1","0")) %>%
#   #filter(shortCall == "0") %>%
#   group_by(studentStudyId) %>%
#   mutate(callNumber = cumsum(inMichelsList))
# 

