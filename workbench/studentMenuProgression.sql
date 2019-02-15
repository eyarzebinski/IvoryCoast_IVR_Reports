SELECT 
treccprod.interactions.id AS `interactions:id`,
treccprod.interactions.user_id AS `interactions:user_id`,
treccprod.interactions.action AS `interactions:action`,
treccprod.interactions.series AS `interactions:series`,
treccprod.interactions.action_step AS `interactions:action_step`,
treccprod.interactions.helper AS `interactions:helper`,
treccprod.interactions.attempt AS `interactions:attempt`,
treccprod.interactions.context AS `interactions:context`,
treccprod.interactions.data AS `interactions:data`,
treccprod.interactions.value AS `interactions:value`,
treccprod.interactions.created_at AS `interactions:created_at`,
treccprod.interactions.updated_at AS `interactions:updated_at`,
treccprod.interactions.persistent AS `interactions:persistent`,
treccprod.interactions.date_created AS `interactions:date_created`,
treccprod.interactions.pending AS `interactions:pending`,
treccprod.interactions.archived AS `interactions:archived`,
treccprod.interactions.deleted AS `interactions:deleted`,
treccprod.interaction_data.id AS `interaction_data:id`,
treccprod.interaction_data.interaction_id AS `interaction_data:interaction_id`,
treccprod.interaction_data.data_key AS `interaction_data:data_key`,
treccprod.interaction_data.value AS `interaction_data:value`,
treccprod.interaction_data.deleted AS `interaction_data:deleted`,
treccprod.interaction_data.created_at AS `interaction_data:created_at`,
treccprod.interaction_data.updated_at AS `interaction_data:updated_at`,
treccprod.users.id AS `users.id`,
treccprod.users.name AS `users.name`,
treccprod.users.email AS `users.email`,
treccprod.users.password AS `users.password`,
treccprod.users.remember_token AS `users.remember_token`,
treccprod.users.created_at AS `users.created_at`,
treccprod.users.updated_at AS `users.updated_at`,
treccprod.users.mprepid AS `users.mprepid`,
treccprod.users.mobile_number AS `users.mobile_number`,
treccprod.users.user_type AS `users.user_type`,
treccprod.users.lang AS `users.lang`,
treccprod.users.consumer_id AS `users.consumer_id`,
treccprod.users.date_created AS `users.date_created`,
treccprod.users.deleted AS `users.deleted`,
treccprod.users.inactive AS `users.inactive`,
treccprod.users.deleted_at AS `users.deleted_at`,
treccprod.cdr_ivr01.uniqueid AS `cdr.uniqueid`,
treccprod.cdr_ivr01.clid AS `cdr.clid`,
treccprod.cdr_ivr01.src AS `cdr.sec`,
treccprod.cdr_ivr01.dst AS `cdr.dst`,
treccprod.cdr_ivr01.dcontext AS `cdr.dcontext`,
treccprod.cdr_ivr01.channel AS `cdr.channel`,
treccprod.cdr_ivr01.dstchannel AS `cdr.dstchannel`,
treccprod.cdr_ivr01.checking AS `cdr.checking`,
treccprod.cdr_ivr01.lastapp AS `cdr.lastapp`,
treccprod.cdr_ivr01.lastdata AS `cdr.lastdata`,
treccprod.cdr_ivr01.calldate AS `cdr.calldate`,
treccprod.cdr_ivr01.callstart AS `cdr.callstart`,
treccprod.cdr_ivr01.duration AS `cdr.duration`,
treccprod.cdr_ivr01.billsec AS `cdr.billsec`,
treccprod.cdr_ivr01.disposition AS `cdr.disposition`,
treccprod.cdr_ivr01.amaflags AS `cdr.amaflags`,
treccprod.cdr_ivr01.uniqueid AS `cdr.uniqueid`,
treccprod.cdr_ivr01.userfield AS `cdr.userfield`
FROM treccprod.interactions
LEFT JOIN treccprod.interaction_stats ON treccprod.interactions.user_id = 
treccprod.interaction_stats.id
LEFT JOIN treccprod.interaction_data ON treccprod.interactions.id = 
treccprod.interaction_data.interaction_id
LEFT JOIN treccprod.users ON treccprod.interactions.user_id = 
treccprod.users.id
LEFT JOIN treccprod.cdr_ivr01 ON treccprod.interaction_data.value = treccprod.cdr_ivr01.uniqueid
WHERE (treccprod.users.user_type='Student' AND
treccprod.users.created_at >= '2019-01-28 00:00:00')
#LEFT JOIN treccprod.cdr ON treccprod.users.uniqueid = 
#treccprod.cdr.uniqueid
ORDER BY treccprod.interactions.created_at DESC;