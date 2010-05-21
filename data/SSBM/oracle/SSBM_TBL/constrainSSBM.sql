
--primary key and foreign key constraints
alter table part
  add primary key (p_partkey);

alter table supplier
  add primary key (s_suppkey);

alter table customer
  add primary key (c_custkey);

alter table dwdate
  add primary key (d_datekey);

alter table lineorder
  add primary key (lo_orderkey, lo_linenumber);
alter table lineorder
  add constraint custconstr foreign key (lo_custkey) references customer(c_custkey);
alter table lineorder
  add constraint partconstr foreign key (lo_partkey) references part(p_partkey);
alter table lineorder
  add constraint suppconstr foreign key (lo_suppkey) references supplier(s_suppkey);
alter table lineorder
  add constraint dateconstr foreign key (lo_orderdate) references dwdate(d_datekey);
-- commitdate should also be foreign key, however we can ignore at
-- for now since the dbgen will generate the commitdate over 19981231
-- which is the largest number in date dimension
--alter table lineorder
--  add foreign key (lo_commitdate) references dwdate(d_datekey);
