SET TIMING ON

SPOOL OFF
SPOOL /data/xuedchen/SSBM_DEV/pub/query_result/q1


/******************************************************
* lineorder orderdate 
*******************************************************/
SELECT lo_oderdate, COUNT (*)
FROM lineorder
WHERE l_orderdate > 19950821
GROUP BY lo_orderdate;

SPOOL OFF
