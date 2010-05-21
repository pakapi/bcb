-- createSSBM.sql
-- SQL statements for table creation for SSBM database

drop table lineorder;
drop table part;
drop table supplier;
drop table dwdate;
drop table customer;



create table part (
  p_partkey     integer     not null,
  p_name        varchar(22) not null,
  p_mfgr        char(6)     not null,
  p_category    char(7)     not null,
  p_brand1      char(9)     not null,
  p_color       varchar(11) not null,
  p_type        varchar(25) not null,
  p_size        integer     not null,
  p_container   char(10)    not null) 
  tablespace ssbm1;

create table supplier (
  s_suppkey     integer     not null,
  s_name        char(25)    not null,
  s_address     varchar(25) not null,
  s_city        char(10)    not null,
  s_nation      char(15)    not null,
  s_region      char(12)    not null,
  s_phone       char(15)    not null)
  tablespace ssbm1;

create table customer (
  c_custkey     integer     not null,
  c_name        varchar(25) not null,
  c_address     varchar(25) not null,
  c_city        char(10)    not null,
  c_nation      char(15)    not null,
  c_region      char(12)    not null,
  c_phone       char(15)    not null,
  c_mktsegment  char(10)    not null)
  tablespace ssbm1;


--date is the reserved keyword in oracle, thus we add prefex dw stands
--for data warehousing

create table dwdate (
  d_datekey            integer     not null,
  d_date               char(19)    not null,
  d_dayofweek          char(10)    not null,
  d_month              char(10)    not null,
  d_year               integer     not null,
  d_yearmonthnum       integer	   not null,
  d_yearmonth          char(8)	   not null,
  d_daynuminweek       integer	   not null,
  d_daynuminmonth      integer     not null,
  d_daynuminyear       integer     not null,
  d_monthnuminyear     integer     not null,
  d_weeknuminyear      integer     not null,
  d_sellingseason      char(13)    not null,
  d_lastdayinweekfl    char(1)     not null,
  d_lastdayinmonthfl   char(1)     not null,
  d_holidayfl          char(1)     not null,
  d_weekdayfl          char(1)     not null)
  tablespace ssbm1;


create table lineorder (
  lo_orderkey          integer     not null,
  lo_linenumber        integer     not null,
  lo_custkey           integer     not null,
  lo_partkey           integer     not null,
  lo_suppkey           integer     not null,
  lo_orderdate         integer     not null,
  lo_orderpriority     char(15)    not null,
  lo_shippriority      char(1)     not null,
  lo_quantity          integer     not null,
  lo_extendedprice     integer     not null,
  lo_ordertotalprice   integer     not null,
  lo_discount          integer     not null,
  lo_revenue           integer     not null,
  lo_supplycost        integer     not null,
  lo_tax               integer     not null,
  lo_commitdate         integer     not null,
  lo_shipmode          char(10)    not null)
  tablespace ssbm1;







