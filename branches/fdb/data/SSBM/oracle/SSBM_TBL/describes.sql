set echo on;
set pause on; 
describe user_tables;
describe all_tables;
describe user_tab_columns;
describe all_constraints;
describe user_triggers;
describe all_tab_privs;
describe all_views;
describe user_updatable_columns;
describe user_catalog;
describe user_tablespaces;
describe user_clusters
describe user_indexes;

-- samle queries
select table_name, column_name
 from user_tab_columns
 where table_name = 'CUSTOMERS' and nullable = 'Y';

select column_name, updatable
 from user_updatable_columns
 where table_name = 'PRODORDS';

select tablespace_name, count(*)
 from user_tables
 group by tablespace_name;

select table_name, constraint_name, constraint_type, search_condition
 from all_constraints where table_name = 'CUSTOMERS';

select cname, rowid from customers where city = 'Dallas';




