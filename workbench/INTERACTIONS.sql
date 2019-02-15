SELECT  
treccprod.interaction_data.id AS `interactionData.id`, 
treccprod.interaction_data.interaction_id AS `interactionData.interaction_id`, 
treccprod.interaction_data.data_key AS `interactionData.data_key`, 
treccprod.interaction_data.value AS `interactionData.value`, 
treccprod.interaction_data.created_at AS `interactionData.created_at`, 
treccprod.interactions.id AS `interactions.id`, 
treccprod.interactions.user_id AS `interactions.user_id`, 
treccprod.interactions.action AS `interactions.action`, 
treccprod.interactions.series AS `interactions.series`, 
treccprod.interactions.action_step AS `interactions.action_step`, 
treccprod.interactions.helper AS `interactions.helper`, 
treccprod.interactions.attempt AS `interactions.attempt`, 
treccprod.interactions.context AS `interactions.context`, 
treccprod.interactions.data AS `interactions.data`, 
treccprod.interactions.value AS `interactions.value`, 
treccprod.interactions.created_at AS `interactions.created_at`, 
treccprod.interactions.persistent AS `interactions.persistent`, 
treccprod.interactions.date_created AS `interactions.date_created`, 
treccprod.interactions.pending AS `interactions.pending`, 
treccprod.user_answer_stats.question_id AS `UAS.questionId`, 
treccprod.user_answer_stats.lesson_id AS `UAS.lessonId`, 
treccprod.cdr_ivr01.uniqueid AS `cdr.uniqueId` 
FROM treccprod.interaction_data 
LEFT JOIN treccprod.interactions ON treccprod.interaction_data.interaction_id = treccprod.interactions.id 
LEFT JOIN treccprod.user_answer_stats ON treccprod.interaction_data.data_key = "question_id" AND treccprod.interaction_data.value = treccprod.user_answer_stats.question_id AND treccprod.user_answer_stats.user_id = treccprod.interactions.user_id 
LEFT JOIN treccprod.cdr_ivr01 ON treccprod.interaction_data.value = treccprod.cdr_ivr01.uniqueid WHERE treccprod.interactions.created_at >= '2019-02-09 00:00:00' 
ORDER BY treccprod.interactions.created_at DESC LIMIT 0, 50000
