drop tablespace ssbm1 including contents and datafiles;
drop tablespace ssbm2 including contents and datafiles;
create tablespace ssbm1 datafile '/disk/sd9e/oracle/OraHome1/oradata/s03/ssbm1.dbf' size 1000M nologging;
create tablespace ssbm2 datafile '/disk/sd9e/oracle/OraHome1/oradata/s03/ssbm2.dbf' size 1000M nologging;

