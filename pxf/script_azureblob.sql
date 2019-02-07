drop external table external_tables.online_sales_fact_azureblob;

CREATE EXTERNAL TABLE external_tables.online_sales_fact_azureblob (
	sale_date_key           int         ,
	ship_date_key           int         ,
	product_key             int         ,
	product_version         int         ,
	customer_key            int         ,
	call_center_key         int         ,
	online_page_key         int         ,
	shipping_key            int         ,
	warehouse_key           int         ,
	promotion_key           int         ,
	pos_transaction_number  int         ,
	sales_quantity          int,
	sales_dollar_amount     float,
	ship_dollar_amount      float,
	net_dollar_amount       float,
	cost_dollar_amount      float,
	gross_profit_dollar_amount float,
	transaction_type        text)
LOCATION ('pxf://cont1@azureblobbr1.blob.core.windows.net/CSV/Online_Sales_Fact.tbl?PROFILE=wasbs:text&server=wasbs')
FORMAT 'TEXT' (delimiter=E'|');

select count(*) from external_tables.online_sales_fact_azureblob;

select * from external_tables.online_sales_fact_azureblob limit 10;
