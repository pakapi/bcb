load data
infile '../../data/part.tbl'
       replace
       into table part
fields terminated by "|"
(p_partkey,p_name,p_mfgr,p_category,p_brand1,p_color,p_type,p_size,p_container)


