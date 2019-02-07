drop external table external_tables.store_sales_fact_minio;

CREATE EXTERNAL TABLE external_tables.store_sales_fact_minio (

CREATE EXTERNAL TABLE external_tables.store_sales_fact_minio (
date_key int ,
        product_key int ,
        product_version int ,
        store_key int ,
        promotion_key int ,
        customer_key int ,
        employee_key int ,
        pos_transaction_number int ,
        sales_quantity int ,
        sales_dollar_amount int ,
        cost_dollar_amount int ,
        gross_profit_dollar_amount int ,
        transaction_type text ,
        transaction_time text ,
        tender_type text )
  LOCATION ('pxf://test/Store_Sales_Fact.tbl?PROFILE=s3:text&SERVER=minio')
FORMAT 'TEXT' (delimiter=E'|');

select count(*) from external_tables.store_sales_fact_minio;

select * from external_tables.store_sales_fact_minio limit 10;
