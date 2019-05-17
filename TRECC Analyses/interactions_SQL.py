# encoding=utf8
import csv
import os
import pickle
import time
from datetime import datetime
from datetime import timedelta

import pymysql

start = time.time()
cnx = pymysql.connect(user='trecc_user', password='treccuser',
                      host='10.20.20.3',
                      database='treccprod', charset='utf8')
cursor = cnx.cursor()
FILE_NAME = 'interaction_id_key.txt'


# cursor.execute('set max_allowed_packet=67108864')\

def get_start_interaction_id():
    dict2 = {}
    if os.path.exists(FILE_NAME):
        file = open(FILE_NAME, 'r+')
        dict2 = pickle.load(file)
    print dict2
    return dict2.get('last_id', 0)


def write_last_id(interaction_id):
    dict = {'last_id': interaction_id}
    file = open(FILE_NAME, 'w')
    pickle.dump(dict, file)
    file.close()

#evelyn fixed?
def find_start_id(table, date, id_name="id", created_at="created_at"):
    sql = "SELECT %s FROM %s ORDER BY %s DESC LIMIT 1" % (id_name, table, id_name)
    #sql = "SELECT %s FROM %s WHERE %s>='%s' LIMIT 1" % (id_name, table, created_at, date)
    #print "CHECK THIS: ", sql
    cursor.execute(sql)
    result = cursor.fetchone()
    return result[0]


def output_csv(sql, file_name, read_type="w+"):
    cnx2 = pymysql.connect(user='trecc_user', password='treccuser',
                           host='10.20.20.3',
                           database='treccprod', charset='utf8')
    cursor2 = cnx2.cursor()
    cursor2.execute(sql)
    # print sql
    result = cursor2.fetchall()
    c = csv.writer(open("%s.csv" % file_name, read_type), delimiter=';')
    if read_type == 'w+':
        keys = [i[0] for i in cursor.description]
        c.writerow(keys)
    for x in result:
        x = ['NULL' if x1 is None else x1 for x1 in x]
        c.writerow([unicode(s).encode("utf-8") for s in x])

    cnx2.close()
    print file_name


STUDY_START_TIME = "2019-02-09 13:00:00"
CDR_START_DATE = "2019-02-20 14:01:28"

ANSWER_START_ID = find_start_id('user_answer_stats', STUDY_START_TIME)
CDR_ID = find_start_id('cdr_ivr01', CDR_START_DATE, "clid", "calldate")
TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M:%S'
today_midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
TODAY = datetime.strftime(today_midnight, TIMESTAMP_FORMAT)
YESTERDAY = datetime.strftime(today_midnight - timedelta(1), TIMESTAMP_FORMAT)
INTERACTIONS_START = YESTERDAY
INTERACTIONS_END = TODAY

INTERACTIONS_START_ID = find_start_id('interaction_data', INTERACTIONS_START, "id", "created_at")   #added "id", "created_at" here to match cdr code. does this work?
INTERACTIONS_END_ID = find_start_id('interaction_data', INTERACTIONS_END, "id", "created_at")       #added "id", "created_at" here to match cdr code. does this work?
USER_START_ID = find_start_id('users', STUDY_START_TIME)

print ""
#print INTERACTIONS_START_ID, INTERACTIONS_END_ID

ANSWER_SQL = """
SELECT
treccprod.user_answer_stats.id AS `UAS:id`,
treccprod.user_answer_stats.completed AS `UAS:completed`,
treccprod.user_answer_stats.correct AS `UAS:correct`,
treccprod.user_answer_stats.correct_option AS `UAS:correct_option`,
treccprod.user_answer_stats.created_at AS `UAS:created_at`, 
treccprod.users.mobile_number AS `users:mobile_number`,
treccprod.users.id AS `users:id`,
treccprod.user_answer_stats.lesson_id AS `UAS:lesson_id`, 
treccprod.user_answer_stats.number_of_attempts AS `UAS:number_of_attempts`, 
treccprod.user_answer_stats.option_given AS `UAS:option_given`, 
treccprod.user_answer_stats.question_id AS `UAS:question_id`, 
treccprod.user_answer_stats.unit_id AS `UAS:unit_id`, 
treccprod.user_answer_stats.updated_at AS `UAS:updated_at`, 
treccprod.user_answer_stats.user_id AS `UAS:user_id`, 
treccprod.user_answer_stats.user_kc_stats_id AS `UAS:user_kc_stats_id`, 
treccprod.cms_options.correct AS `cmsOptions:correct`, 
treccprod.cms_options.id AS `cmsOptions:id`,
treccprod.cms_options.option_text AS `cmsOptions:option_text`, 
treccprod.cms_options.question_id AS `cmsOptions:question_id`,
treccprod.cms_options.token_ids AS `cmsOptions:token_ids`,
treccprod.cms_questions.answer AS `cmsQuestions:answer`,
treccprod.cms_questions.difficulty_level_token AS `cmsQuestions:difficulty_level_token`,
treccprod.cms_questions.difficulty_level_trial AS `cmsQuestions:difficulty_level_trial`,
treccprod.cms_questions.distractor_tokens AS `cmsQuestions:distractor_tokens`,
treccprod.cms_questions.id AS `cmsQuestions:id`,
treccprod.cms_questions.kc_id AS `cmsQuestions:kc_id`,
treccprod.cms_questions.question_number AS `cmsQuestions:question_number`, 
treccprod.cms_questions.question_recordings AS `cmsQuestions:question_recordings`,
treccprod.cms_questions.question_text AS `cmsQuestions:question_text`, 
treccprod.cms_questions.token_a_id AS `cmsQuestions:token_a_id`, 
treccprod.cms_questions.text_output_structure AS `cmsQuestions:text_output_structure`,
treccprod.cms_questions.token_b_id AS `cmsQuestions:token_b_id`, 
treccprod.cms_questions.token_c_id AS `cmsQuestions:token_c_id`, 
treccprod.cms_questions.token_id AS `cmsQuestions:token_id`, 
treccprod.cms_questions.trial_id AS `cmsQuestions:trial_id`, 
treccprod.cms_questions.unit_id AS `cmsQuestions:unit_id`, 
a.id AS `cmsQuestions:token_id_a`,
a.language_id AS `language_id_token_a`, 
a.open_closed AS `open_closed_token_a`, 
a.phonetics_auditory AS `phonetics_auditory_token_a`, 
a.simple_complex AS `simple_complex_token_a`, 
a.soundfile_name AS `soundfile_name_token_a`, 
a.spelling_visual AS `spelling_visual_token_a`, 
a.syllable_structure AS `syllable_structure_token_a`, 
a.token_type_id AS `token_type_id_token_a`,
b.id AS `token_id_b`,
b.language_id AS `language_id_token_b`, 
b.open_closed AS `open_closed_token_b`, 
b.phonetics_auditory AS `phonetics_auditory_token_b`, 
b.simple_complex AS `simple_complex_token_b`, 
b.soundfile_name AS `soundfile_name_token_b`, 
b.spelling_visual AS `spelling_visual_token_b`, 
b.syllable_structure AS `syllable_structure_token_b`, 
b.token_type_id AS `token_type_id_token_b`,
c.id AS `cmsTokens_C:token_id_c`,
c.language_id AS `language_id_token_c`, 
c.open_closed AS `open_closed_token_c`, 
c.phonetics_auditory AS `phonetics_auditory_token_c`, 
c.simple_complex AS `simple_complex_token_c`, 
c.soundfile_name AS `soundfile_name_token_c`, 
c.spelling_visual AS `spelling_visual_token_c`, 
c.syllable_structure AS `syllable_structure_token_c`, 
c.token_type_id AS `token_type_id_token_c`,
treccprod.cms_units.id AS `cmsUnits:id`, 
treccprod.cms_units.theme AS `cmuUnits:theme`, 
treccprod.users.date_created AS `users:date_created`, 
treccprod.users.email AS `users:email`, 
treccprod.users.lang AS `users:lang`, 
treccprod.user_trial_mastery.questions AS `userTrialMastery:questions`,
treccprod.user_trial_mastery.questions_correct AS `userTrialMastery:questions_correct`,
treccprod.user_trial_mastery.trial_mastery_score AS `userTrialMastery:trial_mastery_score`,
treccprod.user_trial_mastery.unit_passed AS `userTrialMastery:unit_passed`,
treccprod.user_trial_mastery.completed AS `userTrialMastery:completed`,
treccprod.user_trial_mastery.trial_started_at AS `userTrialMastery:trial_started_at`,
treccprod.user_trial_mastery.trial_completed_at AS `userTrialMastery:trial_completed_at`,
treccprod.user_trial_mastery.api_threshold AS `userTrialMastery:api_threshold`,
treccprod.user_trial_mastery.api_questions AS `userTrialMastery:api_questions`,
treccprod.user_trial_mastery.api_max_questions AS `userTrialMastery:api_max_questions`,
treccprod.user_trial_mastery.max_questions_reached AS `userTrialMastery:max_questions_reached`,
treccprod.user_kc_stats.id AS `userKCstats:id`, 
treccprod.user_kc_stats.kc_id AS `userKCstats:kc_id`, 
treccprod.user_kc_stats.unit_id AS `userKCstats:unit_id`, 
treccprod.user_kc_stats.user_id AS `userKCstats:user_id`, 
treccprod.user_kc_stats.user_mastery_score AS `userKCstats:user_mastery_score`,
treccprod.users_to_units.id AS `usersToUnits:id`,
treccprod.users_to_units.unit_id AS `usersToUnits:id`,
treccprod.users_to_units.current AS `usersToUnits:current`,
treccprod.users_progress.id AS `userProgress:id`,
treccprod.users_progress.user_id AS `userProgress:user_id`,
treccprod.users_progress.question_ids AS `userProgress:lessonQuestionsRemaining`,
treccprod.users_progress.attempt AS `userProgress:attempt`,
treccprod.users_progress.completed AS `userProgress:completed`,
treccprod.users_progress.current_question AS `userProgress:current_question`,
treccprod.users_progress.created_at AS `userProgress:created_at`,
treccprod.sessions.mobile_number AS `sessions:mobile_number`,
treccprod.sessions.user_id AS `sessions:user_id`
#treccprod.interaction_data.value AS `interactionData:dataValue`
FROM treccprod.user_answer_stats
LEFT JOIN treccprod.cms_options ON treccprod.user_answer_stats.option_given = treccprod.cms_options.id 
LEFT JOIN treccprod.cms_units ON treccprod.user_answer_stats.unit_id = treccprod.cms_units.id 
LEFT JOIN treccprod.sessions ON treccprod.user_answer_stats.user_id = treccprod.sessions.user_id
LEFT JOIN treccprod.users ON treccprod.sessions.mobile_number = treccprod.users.mobile_number
LEFT JOIN treccprod.cms_questions ON treccprod.user_answer_stats.question_id = treccprod.cms_questions.id
LEFT JOIN treccprod.user_kc_stats ON treccprod.user_answer_stats.user_kc_stats_id = treccprod.user_kc_stats.id
LEFT JOIN treccprod.users_to_units ON treccprod.user_answer_stats.unit_id = treccprod.users_to_units.unit_id AND treccprod.users_to_units.user_id = treccprod.sessions.user_id
LEFT JOIN treccprod.cms_tokens ON treccprod.cms_tokens.id = treccprod.cms_questions.token_a_id
LEFT JOIN cms_tokens AS a ON `a`.`id` = treccprod.cms_questions.token_a_id
LEFT JOIN cms_tokens AS b ON `b`.`id` = treccprod.cms_questions.token_b_id
LEFT JOIN cms_tokens AS c ON `c`.`id` = treccprod.cms_questions.token_c_id
LEFT JOIN treccprod.user_trial_mastery ON treccprod.user_trial_mastery.trial_id = treccprod.cms_questions.trial_id AND treccprod.user_trial_mastery.user_id = treccprod.user_answer_stats.user_id
LEFT JOIN treccprod.users_progress ON treccprod.users_progress.id = treccprod.user_answer_stats.lesson_id
#LEFT JOIN treccprod.interaction_data ON treccprod.interaction_data.data_key = 'question_id' AND treccprod.interaction_data.value = treccprod.user_answer_stats.question_id
#LEFT JOIN treccprod.interaction_data ON treccprod.interaction_data.value = treccprod.user_answer_stats.question_id
WHERE treccprod.user_answer_stats.id >= %s
ORDER BY treccprod.user_answer_stats.id DESC;
""" % (ANSWER_START_ID)

INTERACTIONS_SQL = """
SELECT
treccprod.interaction_data.id AS `interactionData.id`,
treccprod.interaction_data.interaction_id AS `interactionData.interaction_id`,
treccprod.interaction_data.data_key AS `interactionData.data_key`,
treccprod.interaction_data.value AS `interactionData.value`,
treccprod.interaction_data.created_at AS `interactionData.created_at`,
treccprod.interactions.id AS `interactions.id`,
treccprod.interactions.user_id AS `interactions.user_id`,
treccprod.interactions.action AS `interactions.action`,
treccprod.interactions.attempt AS `interactions.attempt`,
treccprod.interactions.data AS `interactions.data`,
treccprod.interactions.value AS `interactions.value`,
treccprod.interactions.created_at AS `interactions.created_at`,
treccprod.interactions.persistent AS `interactions.persistent`,
treccprod.interactions.date_created AS `interactions.date_created`,
treccprod.interactions.pending AS `interactions.pending`
#treccprod.user_answer_stats.question_id AS `UAS.questionId`,
#treccprod.user_answer_stats.lesson_id AS `UAS.lessonId`,
#treccprod.cdr_ivr01.uniqueid AS `cdr.uniqueId`
FROM treccprod.interaction_data
LEFT JOIN treccprod.interactions ON treccprod.interaction_data.interaction_id = treccprod.interactions.id
#LEFT JOIN treccprod.user_answer_stats ON treccprod.interaction_data.data_key = 'question_id' AND treccprod.interaction_data.value = treccprod.user_answer_stats.question_id AND treccprod.interactions.user_id = treccprod.user_answer_stats.user_id
#LEFT JOIN treccprod.cdr_ivr01 ON treccprod.interaction_data.value = treccprod.cdr_ivr01.uniqueid
WHERE treccprod.interaction_data.id >= %s AND treccprod.interaction_data.id < %s
#WHERE treccprod.interaction_data.created_at >= '2019-02-27 00:00:00'
ORDER BY treccprod.interaction_data.id ASC;
"""

USER_SQL = """SELECT * FROM treccprod.users WHERE treccprod.users.id >= %s;""" % (USER_START_ID)

CDR_SQL = """SELECT * FROM treccprod.cdr_ivr01 WHERE treccprod.cdr_ivr01.clid >= %s ORDER BY treccprod.cdr_ivr01.clid DESC;""" % (
    CDR_ID)

print TODAY, YESTERDAY
print " "
cnx.close()
#print ANSWER_SQL
# output_csv(ANSWER_SQL, "answers-output")


#print " "

#print USER_SQL
# output_csv(USER_SQL, "users-output")

#print ""

#print CDR_SQL
# removing cdr
# output_csv(CDR_SQL, "cdr-output")

#print ""

# print INTERACTIONS_SQL
INTERACTIONS_START = get_start_interaction_id()
if INTERACTIONS_START:
    INTERACTIONS_START_ID = INTERACTIONS_START
LIMIT = 200
END = 0
print "Full range: ", INTERACTIONS_START_ID, INTERACTIONS_END_ID

for i in range(INTERACTIONS_START_ID, INTERACTIONS_END_ID, LIMIT):
    START = i
    END = i + LIMIT
    INTERACTIONS_SQL_NEW = INTERACTIONS_SQL % (START, END)
    write_last_id(END)
    print "Current range: ", START, END
    output = output_csv(INTERACTIONS_SQL_NEW,
                        "interactions-file-%s-%s-%s" % (INTERACTIONS_START_ID, INTERACTIONS_END_ID, LIMIT),
                        read_type="a+")
if END:
    if END > INTERACTIONS_END_ID:
        #END -= LIMIT
        END = INTERACTIONS_END_ID
    INTERACTIONS_SQL_NEW = INTERACTIONS_SQL % (END, INTERACTIONS_END_ID)
    print "Final range: ", END, INTERACTIONS_END_ID #changed START to END
    write_last_id(INTERACTIONS_END_ID)
    #output = output_csv(INTERACTIONS_SQL_NEW,                                                                      #write this result to a csv
    #                    "interactions-file-%s-%s-%s" % (INTERACTIONS_START_ID, INTERACTIONS_END_ID, LIMIT),        #and keep the title the same as in the for loop
    #                    read_type="a+")


end = time.time()

print "Duration (in seconds): ", end - start
