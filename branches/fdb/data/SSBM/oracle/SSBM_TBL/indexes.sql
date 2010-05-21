--possible bitmap indexes
CREATE BITMAP INDEX LO_BMX_PARTKEY ON lineorder(lo_partkey) tablespace ssbm2;
CREATE BITMAP INDEX LO_BMX_SUPPKEY ON lineorder(lo_suppkey) tablespace ssbm2;
CREATE BITMAP INDEX LO_BMX_CUSTKEY ON lineorder(lo_custkey) tablespace ssbm2;
CREATE BITMAP INDEX LO_BMX_ORDERDATE ON lineorder(lo_orderdate) tablespace ssbm2;
