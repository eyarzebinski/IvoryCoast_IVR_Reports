SELECT *
#treccprod.cdr.amaflags,
#treccprod.cdr.billsec,
#treccprod.cdr.calldate,
#treccprod.cdr.callend,
#treccprod.cdr.callstart,
#treccprod.cdr.channel,
#treccprod.cdr.checking,
#treccprod.cdr.clid,
#treccprod.cdr.dcontext,
#treccprod.cdr.disposition,
#treccprod.cdr.dst,
#treccprod.cdr.dstchannel,
#treccprod.cdr.duration,
#treccprod.cdr.lastapp,
#treccprod.cdr.lastdata,
#treccprod.cdr.src,
#treccprod.cdr.uniqueid,
#treccprod.cdr.userfield
FROM treccprod.cdr_ivr01
WHERE treccprod.cdr_ivr01.calldate >= '2019-02-09 00:00:00'
ORDER BY treccprod.cdr_ivr01.callstart DESC 