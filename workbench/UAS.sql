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
treccprod.user_unit_stats.user_id AS `userUnitStats:user_id`,
treccprod.user_unit_stats.number_of_attempts AS `userUnitStats:questions_attempted`, 
treccprod.user_unit_stats.unit_id AS `userUnitStats:unit_id`, 
treccprod.user_unit_stats.average_user_mastery_score AS `userUnitStats:average_user_mastery_score`, 
treccprod.user_unit_stats.completed AS `userUnitStats:completed`, 
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
LEFT JOIN treccprod.user_unit_stats ON treccprod.user_unit_stats.user_id = treccprod.sessions.user_id AND treccprod.user_unit_stats.unit_id = treccprod.users_to_units.unit_id
LEFT JOIN treccprod.users_progress ON treccprod.users_progress.id = treccprod.user_answer_stats.lesson_id
#LEFT JOIN treccprod.interaction_data ON treccprod.interaction_data.data_key = 'question_id' AND treccprod.interaction_data.value = treccprod.user_answer_stats.question_id
#LEFT JOIN treccprod.interaction_data ON treccprod.interaction_data.value = treccprod.user_answer_stats.question_id
WHERE (treccprod.users.date_created >= '2019-02-01 00:00:00' AND 
treccprod.user_answer_stats.created_at >= '2019-02-01 00:00:00')
ORDER BY treccprod.user_answer_stats.created_at DESC;