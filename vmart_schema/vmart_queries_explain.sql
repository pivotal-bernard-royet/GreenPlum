--validation from explain plan
select set_vertica_options('OPT', 'PLAN_OUTPUT_VERBOSE');

-- vmart_query_01.sql
-- FROM clause subquery
-- Return the values for five products with the 
-- lowest-fat content in the Dairy department

\o | cat > $TMPDIR/explain.out

explain SELECT fat_content 
FROM (SELECT DISTINCT fat_content 
      FROM product_dimension 
      WHERE department_description 
      IN ('Dairy') 
      ORDER BY fat_content) AS food 
      LIMIT 5;

\o
\! sed -n '/ Access Path/,/----/p' < $TMPDIR/explain.out

-- vmart_query_02.sql
-- WHERE clause subquery
-- Asks for all orders placed by stores located in Massachusetts 
-- and by vendors located elsewhere before March 1, 2003:

\o | cat > $TMPDIR/explain.out

explain SELECT order_number, date_ordered
FROM store.store_orders_fact orders
WHERE orders.store_key IN
   (SELECT store_key
    FROM store.store_dimension
    WHERE store_state = 'MA') 
AND orders.vendor_key NOT IN
   (SELECT vendor_key
    FROM public.vendor_dimension
    WHERE vendor_state = 'MA')
AND date_ordered < '2003-03-01';

\o
\! sed -n '/ Access Path/,/----/p' < $TMPDIR/explain.out

-- vmart_query_03.sql
-- Noncorrelated subquery
-- Requests female and male customers with the maximum 
-- annual income from customers

\o | cat > $TMPDIR/explain.out

explain SELECT customer_name
FROM public.customer_dimension
WHERE (customer_gender, annual_income) IN
      (SELECT customer_gender, MAX(annual_income)
       FROM public.customer_dimension
       GROUP BY customer_gender);

\o
\! sed -n '/ Access Path/,/----/p' < $TMPDIR/explain.out

-- vmart_query_04.sql
-- IN predicate
-- Find all products supplied by stores in MA

\o | cat > $TMPDIR/explain.out

explain SELECT DISTINCT s.product_key, p.product_description
FROM store.store_sales_fact s, public.product_dimension p
WHERE s.product_key = p.product_key
    AND s.product_version = p.product_version
    AND s.store_key IN
	  (SELECT store_key
	   FROM store.store_dimension
	   WHERE store_state = 'MA')
ORDER BY s.product_key;

\o
\! sed -n '/ Access Path/,/----/p' < $TMPDIR/explain.out

-- vmart_query_05.sql
-- EXISTS predicate
-- Get a list of all the orders placed by all stores on 
-- January 2, 2003 for the vendors with records in the 
-- vendor_dimension table 

\o | cat > $TMPDIR/explain.out

explain SELECT store_key, order_number, date_ordered
FROM store.store_orders_fact
WHERE EXISTS
  (SELECT 1
   FROM public.vendor_dimension
   WHERE public.vendor_dimension.vendor_key 
   = store.store_orders_fact.vendor_key)
AND date_ordered = '2003-01-02';

\o
\! sed -n '/ Access Path/,/----/p' < $TMPDIR/explain.out

-- vmart_query_06.sql
-- EXISTS predicate
-- Orders placed by the vendor who got the best deal 
-- on January 4, 2004

\o | cat > $TMPDIR/explain.out

explain SELECT store_key, order_number, date_ordered
FROM store.store_orders_fact ord,
     public.vendor_dimension vd
WHERE ord.vendor_key = vd.vendor_key
      AND vd.deal_size IN
         (SELECT MAX(deal_size)
          FROM public.vendor_dimension)
     AND date_ordered = '2004-01-04';

\o
\! sed -n '/ Access Path/,/----/p' < $TMPDIR/explain.out

-- vmart_query_07.sql
-- Multicolumn subquery
-- Which products have the highest cost, 
-- grouped by category and department 

\o | cat > $TMPDIR/explain.out

explain SELECT product_description
FROM public.product_dimension
WHERE (category_description, department_description, product_cost) IN
   (SELECT category_description, department_description, MAX(product_cost)
    FROM product_dimension
    GROUP BY category_description, department_description);

\o
\! sed -n '/ Access Path/,/----/p' < $TMPDIR/explain.out
    
-- vmart_query_08.sql
-- Using pre-join projections to answer subqueries
-- between online_sales_fact and online_page_dimension

\o | cat > $TMPDIR/explain.out

explain SELECT page_description, page_type, start_date, end_date
FROM online_sales.online_sales_fact f, online_sales.online_page_dimension d
WHERE f.online_page_key = d.online_page_key 
AND page_number IN
  (SELECT MAX(page_number)
    FROM online_sales.online_page_dimension)
AND page_type = 'monthly' AND start_date = '2006-05-11';

\o
\! sed -n '/ Access Path/,/----/p' < $TMPDIR/explain.out

-- vmart_query_09.sql
-- Equi join
-- Joins online_sales_fact table and the call_center_dimension 
-- table with the ON clause

\o | cat > $TMPDIR/explain.out

explain SELECT * 
FROM online_sales.online_sales_fact
INNER JOIN online_sales.call_center_dimension 
ON (online_sales.online_sales_fact.call_center_key 
    = online_sales.call_center_dimension.call_center_key
    AND sale_date_key = 156); 

\o
\! sed -n '/ Access Path/,/----/p' < $TMPDIR/explain.out

