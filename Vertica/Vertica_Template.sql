select omsmerchantid, institutionid,merchantcategoryid, segmentid, createdate,updatedate,assignedname, name from cdw.vmerchantunmatchedauth
where institutionid = 2726 and (merchantcategoryid in (7230,5999,7298,7299) and
regexp_like(name, '\bgreat?\s*cl|\bgr?t?\s*cl','b') and not  regexp_like(name, '\baramark|paypal', 'b')
and segmentid not in (35110

))
limit 30000

-----------------------------------------------------------------------------------------
select omsmerchantid, institutionid,merchantcategoryid, segmentid, createdate,updatedate,assignedname, name from cdw.vmerchantunmatchedauth
where institutionid = 2726 and
regexp_like(name, '\bcrate?\s*.?\s*bar','b') and not  regexp_like(name, '\baramark', 'b')
and segmentid not in (50136,
43350,
22305


)
limit 30000

-----------------------------------------------------------------------------------------
select omsmerchantid, institutionid,merchantcategoryid, segmentid, createdate,updatedate,assignedname, name from cdw.vmerchantunmatched
where institutionid not in (2002) and
regexp_like(name, '\bbest?\s*buy','b')
and segmentid not in (10010,10020,10100,10130,10110,10050,10060,10070,10140,10160,10230,10245,10251,20150,30510,30508,50022,50023,50024,52927,70515,99998


)
limit 30000