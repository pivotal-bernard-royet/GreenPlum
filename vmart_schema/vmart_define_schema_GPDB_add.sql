drop table if exists store.store_sales_fact_part;

CREATE TABLE store.store_sales_fact_part (
date_key                date,
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
) WITH (appendonly=true, orientation=column,compresstype=rle_type)
DISTRIBUTED BY (date_key)
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

-- INDEXES Optionnal

drop index if exists store.store_sales_fact_part_ix1;
drop index if exists store.store_sales_fact_part_ix2;
drop index if exists store.store_sales_fact_part_ix3;
drop index if exists store.store_sales_fact_part_ix4;
drop index if exists store.store_sales_fact_part_ix5;
drop index if exists store.store_sales_fact_part_ix6;

CREATE INDEX store_sales_fact_part_ix1 ON store.store_sales_fact_part (date_key);
CREATE INDEX store_sales_fact_part_ix2 ON store.store_sales_fact_part (product_key,product_version);
CREATE INDEX store_sales_fact_part_ix3 ON store.store_sales_fact_part (store_key);
CREATE INDEX store_sales_fact_part_ix4 ON store.store_sales_fact_part (promotion_key);
CREATE INDEX store_sales_fact_part_ix5 ON store.store_sales_fact_part (customer_key);
CREATE INDEX store_sales_fact_part_ix6 ON store.store_sales_fact_part (employee_key);