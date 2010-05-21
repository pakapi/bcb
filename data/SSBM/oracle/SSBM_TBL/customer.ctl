load data
infile '../../data/customer.tbl'
       replace
       into table customer
fields terminated by "|"
(c_custkey,c_name,c_address,c_city,c_nation,c_region,c_phone,c_mktsegment)


