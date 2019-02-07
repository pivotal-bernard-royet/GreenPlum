drop table store.store_orders_fact_part;

CREATE TABLE store.store_orders_fact_part (
	product_key             integer,
	product_version         integer,
	store_key               integer,
	vendor_key              integer,
	employee_key            integer,
	order_number            integer,
	date_ordered            date,
	date_shipped            date,
	expected_delivery_date  date,
	date_delivered          date,
	quantity_ordered        integer,
	quantity_delivered      integer,
	shipper_name            varchar(32),
	unit_price              integer,
	shipping_cost           integer,
	total_order_cost        integer,
	quantity_in_stock       integer,
	reorder_level           integer,
	overstock_ceiling       integer
DISTRIBUTED BY (product_key,product_version,store_key,vendor_key,employee_key,order_number)
  PARTITION BY RANGE (date_ordered)
(PARTITION yr_1 START (date '2012-01-01') INCLUSIVE,
PARTITION yr_2 START (date '2013-01-01') INCLUSIVE,
PARTITION yr_3 START (date '2014-01-01') INCLUSIVE,
PARTITION yr_4 START (date '2015-01-01') INCLUSIVE,
PARTITION yr_5 START (date '2016-01-01') INCLUSIVE,
PARTITION yr_6 START (date '2017-01-01') INCLUSIVE,
PARTITION yr_7 START (date '2018-01-01') INCLUSIVE
   END (date '2018-12-31') INCLUSIVE
EVERY (INTERVAL '1 year'));

insert into store.store_orders_fact_part select * from store.store_orders_fact;

DROP EXTERNAL TABLE store.store_orders_fact_part_ext_w;

CREATE WRITABLE EXTERNAL TABLE store.store_orders_fact_part_ext_w ( LIKE store.store_orders_fact_part_1_prt_yr_1 )
  LOCATION ( 'pxf://cont1@azureblobbr1.blob.core.windows.net/CSV/store_orders_fact_part_1_prt_1.tbl?PROFILE=wasbs:text&server=wasbs')
  FORMAT 'csv' 
  DISTRIBUTED BY (product_key,product_version,store_key,vendor_key,employee_key,order_number);

DROP EXTERNAL TABLE store.store_orders_fact_part_ext_r;

CREATE EXTERNAL TABLE store.store_orders_fact_part_ext_r ( LIKE store.store_orders_fact_part_1_prt_yr_1 )
  LOCATION ( 'pxf://cont1@azureblobbr1.blob.core.windows.net/CSV/store_orders_fact_part_1_prt_1.tbl?PROFILE=wasbs:text&server=wasbs')
  FORMAT 'csv' ;

INSERT INTO store.store_orders_fact_part_ext_w SELECT * from store.store_orders_fact_part_1_prt_yr_1;

ALTER TABLE store.store_orders_fact_part ALTER PARTITION yr_1 
   EXCHANGE PARTITION yr_1 
   WITH TABLE store.store_orders_fact_part_ext_r WITHOUT VALIDATION;

DROP TABLE store.store_orders_fact_part_ext_r;

ALTER TABLE store.store_orders_fact_part RENAME PARTITION yr_1 to yr_1_ext;
