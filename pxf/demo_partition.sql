-- Queries

-- Minio (50 000 000 rows)
select count(*) from store.store_sales_fact;
select count(*) from external_tables.store_sales_fact_minio;

-- Azure Blob (5 000 000 rows)
select count(*) from online_sales.online_sales_fact;
select count(*) from external_tables.online_sales_fact_azureblob;

-- Partition small Azure (300 000 rows)
select count(*) from store.store_orders_fact;
select count(*) from store.store_orders_fact_part;

select count(*) from store.store_orders_fact
where date(date_ordered) between (date '2012-01-01') and (date '2012-12-31');

select count(*) from store.store_orders_fact_part
where date(date_ordered) between (date '2012-01-01') and (date '2012-12-31');

-- Partition medium Azure (5 000 000 rows)
select count(*) from online_sales.online_sales_fact;
select count(*) from online_sales.online_sales_fact_part;

select count(*),calendar_year from online_sales.online_sales_fact s, date_dimension d
where s.ship_date_key=d.date_key 
group by calendar_year order by calendar_year;

select count(*),calendar_year from online_sales.online_sales_fact_part s, date_dimension d
where s.ship_date_key=d.date_key 
group by calendar_year order by calendar_year;

select count(*) from online_sales.online_sales_fact s, date_dimension d
where s.ship_date_key=d.date_key 
and calendar_year=2012;

select count(*) from online_sales.online_sales_fact_part s, date_dimension d
where s.ship_date_key=d.date_key 
and calendar_year=2012;

-- Partition large Azure (50 000 000 rows)
select count(*) from store.store_sales_fact;
select count(*) from store.store_sales_fact_part;

select count(*),calendar_year from store.store_sales_fact s, date_dimension d
where s.date_key=d.date_key 
group by calendar_year order by calendar_year;

select count(*),calendar_year from store.store_sales_fact_part s, date_dimension d
where s.date_key=d.date_key 
group by calendar_year order by calendar_year;

select count(*) from store.store_sales_fact s, date_dimension d
where s.date_key=d.date_key 
and calendar_year=2012;

select count(*) from store.store_sales_fact_part s, date_dimension d
where s.date_key=d.date_key 
and calendar_year=2012;

-- Tableau demo : vmart_demo_azure_big  vmart_demo_azure_big_part


