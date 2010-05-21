load data
infile '../../data/supplier.tbl'
       replace
       into table supplier
fields terminated by "|"
(s_suppkey,s_name,s_address,s_city,s_nation,s_region,s_phone)


