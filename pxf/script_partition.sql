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
overstock_ceiling       integer)
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

---- 
select min(ship_date_key),max(ship_date_key),calendar_year from online_sales_fact o, date_dimension d
where o.ship_date_key=d.date_key
group by calendar_year;

drop table online_sales.online_sales_fact_part;

CREATE TABLE online_sales.online_sales_fact_part (
sale_date_key           integer,
ship_date_key           integer,
product_key             integer,
product_version         integer,
customer_key            integer,
call_center_key         integer,
online_page_key         integer,
shipping_key            integer,
warehouse_key           integer,
promotion_key           integer,
pos_transaction_number  integer,
sales_quantity          integer,
sales_dollar_amount     float,
ship_dollar_amount      float,
net_dollar_amount       float,
cost_dollar_amount      float,
gross_profit_dollar_amount float,
transaction_type        varchar(16)
) WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (sale_date_key,ship_date_key,product_key,product_version,customer_key,call_center_key,online_page_key,
shipping_key,warehouse_key,promotion_key)
PARTITION BY RANGE (ship_date_key)
(PARTITION yr_1 START (2) INCLUSIVE,
PARTITION yr_2 START (367) INCLUSIVE,
PARTITION yr_3 START (732) INCLUSIVE,
PARTITION yr_4 START (1097) INCLUSIVE,
PARTITION yr_5 START (1462) INCLUSIVE,
PARTITION yr_6 START (1828) INCLUSIVE,
PARTITION yr_7 START (2193) INCLUSIVE
END (2557) INCLUSIVE
);

insert into online_sales.online_sales_fact_part select * from online_sales.online_sales_fact;

DROP EXTERNAL TABLE online_sales.online_sales_fact_part_ext_w;

CREATE WRITABLE EXTERNAL TABLE online_sales.online_sales_fact_part_ext_w ( LIKE online_sales.online_sales_fact_part_1_prt_yr_1 )
LOCATION ( 'pxf://cont1@azureblobbr1.blob.core.windows.net/CSV/online_sales_fact_part_1_prt_yr_1.tbl?PROFILE=wasbs:text&server=wasbs')
FORMAT 'csv' 
DISTRIBUTED BY (sale_date_key,ship_date_key,product_key,product_version,customer_key,call_center_key,online_page_key,
shipping_key,warehouse_key,promotion_key);

DROP EXTERNAL TABLE online_sales.online_sales_fact_part_ext_r;

CREATE EXTERNAL TABLE online_sales.online_sales_fact_part_ext_r ( LIKE online_sales.online_sales_fact_part_1_prt_yr_1  )
LOCATION ( 'pxf://cont1@azureblobbr1.blob.core.windows.net/CSV/online_sales_fact_part_1_prt_yr_1.tbl?PROFILE=wasbs:text&server=wasbs')
FORMAT 'csv' ;

INSERT INTO online_sales.online_sales_fact_part_ext_w SELECT * from online_sales.online_sales_fact_part_1_prt_yr_1;

ALTER TABLE online_sales.online_sales_fact_part ALTER PARTITION yr_1 
   EXCHANGE PARTITION yr_1 
   WITH TABLE online_sales.online_sales_fact_part_ext_r  WITHOUT VALIDATION;

DROP TABLE online_sales.online_sales_fact_part_ext_r;

ALTER TABLE online_sales.online_sales_fact_part RENAME PARTITION yr_1 to yr_1_ext;

---- Example 

-- CleanUP
drop table store.store_sales_fact_part;
DROP EXTERNAL TABLE store.store_sales_fact_part_ext_r;
DROP EXTERNAL TABLE store.store_sales_fact_part_ext_w;

-- Get the date repartition into the table

select min(s.date_key),max(s.date_key),calendar_year from store.store_sales_fact s, date_dimension d
where s.date_key=d.date_key group by calendar_year order by calendar_year;
--  min  | max  | calendar_year
-- ------+------+---------------
--     1 |  366 |          2012
--   367 |  731 |          2013
--   732 | 1096 |          2014
--  1097 | 1461 |          2015
--  1462 | 1827 |          2016
--  1828 | 2192 |          2017
--  2193 | 2557 |          2018
-- (7 rows)


-- Create a partitioned table from the original table


CREATE TABLE store.store_sales_fact_part (
date_key                integer,
product_key             integer,
product_version         integer,
store_key               integer,
promotion_key           integer,
customer_key            integer,
employee_key            integer,
pos_transaction_number  integer,
sales_quantity          integer,
sales_dollar_amount     integer,
cost_dollar_amount      integer,
gross_profit_dollar_amount integer,
transaction_type        varchar(16),
transaction_time        time,
tender_type             varchar(8)
) WITH (appendonly=true, orientation=column)
DISTRIBUTED BY (date_key,product_key,product_version,store_key,promotion_key,customer_key,employee_key)
PARTITION BY RANGE (date_key)
(PARTITION yr_1 START (1) INCLUSIVE,
PARTITION yr_2 START (366) INCLUSIVE,
PARTITION yr_3 START (731) INCLUSIVE,
PARTITION yr_4 START (1096) INCLUSIVE,
PARTITION yr_5 START (1461) INCLUSIVE,
PARTITION yr_6 START (1827) INCLUSIVE,
PARTITION yr_7 START (2192) INCLUSIVE
END (2557) INCLUSIVE
);

-- \d+ store.store_sales_fact_part;

-- Copy the data 

insert into store.store_sales_fact_part select * from store.store_sales_fact;

--	Create a writable external table.
--	This CREATE WRITABLE EXTENAL TABLE command creates a writable external table with the same columns as the partitioned table.

CREATE WRITABLE EXTERNAL TABLE store.store_sales_fact_part_ext_w ( LIKE store.store_sales_fact_part_1_prt_yr_1 )
LOCATION ( 'pxf://cont1@azureblobbr1.blob.core.windows.net/CSV/store_sales_fact_part_1_prt_yr_1.tbl?PROFILE=wasbs:text&server=wasbs')
FORMAT 'csv' 
DISTRIBUTED BY (date_key,product_key,product_version,store_key,promotion_key,customer_key,employee_key);

--Create a readable external table that reads the data from that destination of the writable external table created in the previous step.
--This CREATE EXTENAL TABLE create a readable external that uses the same external data as the writable external data.

CREATE EXTERNAL TABLE store.store_sales_fact_part_ext_r ( LIKE store.store_sales_fact_part_1_prt_yr_1  )
LOCATION ( 'pxf://cont1@azureblobbr1.blob.core.windows.net/CSV/store_sales_fact_part_1_prt_yr_1.tbl?PROFILE=wasbs:text&server=wasbs')
FORMAT 'csv' ;

--Copy the data from the leaf child partition into the writable external table.
--This INSERT command copies the data from the child leaf partition table of the partitioned table into the external table.

INSERT INTO store.store_sales_fact_part_ext_w SELECT * from store.store_sales_fact_part_1_prt_yr_1;

--Exchange the existing leaf child partition with the external table.
--This ALTER TABLE command specifies the EXCHANGE PARTITION clause to switch the readable external table and the leaf child partition.

ALTER TABLE store.store_sales_fact_part ALTER PARTITION yr_1 
   EXCHANGE PARTITION yr_1 
   WITH TABLE store.store_sales_fact_part_ext_r  WITHOUT VALIDATION;

-- Drop the table that was rolled out of the partitioned table.

DROP TABLE store.store_sales_fact_part_ext_r;

-- optional : You can rename the name of the leaf child partition to indicate that xx_1_part_yr_1 is an external table.

ALTER TABLE store.store_sales_fact_part RENAME PARTITION yr_1 to yr_1_ext;