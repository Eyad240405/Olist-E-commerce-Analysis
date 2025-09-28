use [Olist]

---unique customers placed orders
select COUNT(DISTINCT customer_id) as unique_customers
from [dbo].[olist_orders_dataset]

---top 10 most sold product categories
select top(10) [product_category_name], COUNT(*) as total_items_sold
from [dbo].[olist_products_dataset] p join [dbo].[olist_order_items_dataset] oi on p.product_id = oi.product_id
group by [product_category_name] 
order by total_items_sold desc

---sellers generated the highest revenue
select [seller_id], SUM([price]) as total_revenue
from [dbo].[olist_order_items_dataset]	
group by [seller_id]
order by 2 desc

---the average delivery delay
select AVG(DATEDIFF(Day, [order_delivered_customer_date], [order_estimated_delivery_date])) as average_delivery_delay
from [dbo].[olist_orders_dataset]
where [order_delivered_customer_date] is not null AND [order_estimated_delivery_date] is not null

---payment methods are most common
select  [payment_type], COUNT(*)
from [dbo].[olist_order_payments_dataset]
group by [payment_type]
order by 2 desc

---monthly trend of orders and revenue
select FORMAT(o.order_purchase_timestamp, 'yyyy-MM') as month_, COUNT(o.order_id) as total_orders, SUM(oi.price) as total_revenue 
from [dbo].[olist_orders_dataset] o join [dbo].[olist_order_items_dataset] oi on o.order_id = oi.order_id
where o.order_purchase_timestamp is not null
group by  FORMAT(o.order_purchase_timestamp, 'yyyy-MM')
order by 1

---average review score by product category
select p.product_category_name, AVG(r.review_score) as average_scores
from [dbo].[olist_order_reviews_dataset] r join [dbo].[olist_orders_dataset] o on r.order_id = o.order_id
join [dbo].[olist_order_items_dataset] oi on o.order_id = oi.order_id
join [dbo].[olist_products_dataset] p on oi.product_id = p.product_id
WHERE r.review_score IS NOT NULL
group by  p.product_category_name

---top customers by spending
select [customer_id], sum([price]) as total_spent
from [dbo].[olist_orders_dataset] o join [dbo].[olist_order_items_dataset] oi on o.order_id = oi.order_id
group by [customer_id]
order by 2 desc

---states have the highest number of orders
select [customer_state], COUNT(o.order_id) as orders
from [dbo].[olist_customers_dataset] c join [dbo].[olist_orders_dataset] o on c.customer_id = o.customer_id
group by [customer_state]
order by 2 desc

---window functions to rank sellers by revenue within each state
select 
    seller_id,
    seller_state,
    total_revenue,
    RANK() OVER (PARTITION BY seller_state ORDER BY total_revenue DESC) AS revenue_rank
from (
    SELECT 
        s.seller_id,
        s.seller_state,
        SUM(oi.price) AS total_revenue
    FROM 
        dbo.olist_order_items_dataset oi
    JOIN 
        dbo.olist_sellers_dataset s ON oi.seller_id = s.seller_id
    GROUP BY 
        s.seller_id, s.seller_state
) AS seller_revenue;

---In year 2018, flag each seller as (below target – within target – above target)  based on no of sold items and revenue
WITH seller_stats AS (
    SELECT 
        oi.seller_id,
        COUNT(*) AS items_sold,
        SUM(oi.price) AS total_revenue
    FROM 
        dbo.olist_order_items_dataset oi
    JOIN 
        dbo.olist_orders_dataset o ON oi.order_id = o.order_id
    WHERE 
        YEAR(o.order_purchase_timestamp) = 2018
    GROUP BY 
        oi.seller_id
),
target_values AS (
    SELECT 
        AVG(items_sold) AS avg_items_sold,
        AVG(total_revenue) AS avg_revenue
    FROM 
        seller_stats
)
SELECT 
    s.seller_id,
    s.items_sold,
    s.total_revenue,
    CASE 
        WHEN s.items_sold < t.avg_items_sold AND s.total_revenue < t.avg_revenue THEN 'below target'
        WHEN s.items_sold >= t.avg_items_sold AND s.total_revenue >= t.avg_revenue THEN 'above target'
        ELSE 'within target'
    END AS performance_flag
FROM seller_stats s CROSS JOIN target_values t
ORDER BY 
    s.total_revenue DESC;

---Customer Order Summary View
create view customer_order_summary
AS
select c.[customer_id], c.customer_unique_id, c.customer_city, c.customer_state, COUNT(DISTINCT o.order_id) as total_orders, COUNT(oi.order_id) as total_items, SUM(oi.price) total_spent
from [dbo].[olist_customers_dataset] c join [dbo].[olist_orders_dataset] o on c.customer_id = o.customer_id
left join [dbo].[olist_order_items_dataset] oi on o.order_id = oi.order_id
group by c.[customer_id], c.customer_unique_id, c.customer_city, c.customer_state

select * from [dbo].[customer_order_summary]

---Seller Performance View
create view seller_performance
AS
select s.seller_id, s.seller_city, s.seller_state, COUNT(DISTINCT o.order_id) as total_orders, COUNT(oi.order_id) as total_items, SUM(oi.price) total_revenue
from [dbo].[olist_sellers_dataset] s join [dbo].[olist_order_items_dataset] oi on s.seller_id = oi.seller_id
left join [dbo].[olist_orders_dataset] o on oi.order_id = o.order_id
group by s.seller_id, s.seller_city, s.seller_state

select * from seller_performance

---Product Category Sales View
create view product_category_sales
AS
select p.product_id, p.product_category_name, COUNT(DISTINCT o.order_id) as total_orders,
COUNT(oi.order_id) as total_items, SUM(oi.price) total_revenue, AVG(r.review_score) as Avg_revenue_score
from [dbo].[olist_products_dataset] p 
join [dbo].[olist_order_items_dataset] oi on p.product_id = oi.product_id
join [dbo].[olist_orders_dataset] o on oi.order_id = o.order_id
join [dbo].[olist_order_reviews_dataset] r on o.order_id = r.order_id
group by p.product_id, p.product_category_name

select * from product_category_sales

---Get top 10 customers by total spend
create procedure cusromers_by_total_spend
@top_n int = 10
AS
BEGIN
	select top(@top_n)[customer_id], sum([price]) as total_spent
	from [dbo].[olist_orders_dataset] o join [dbo].[olist_order_items_dataset] oi on o.order_id = oi.order_id
	group by [customer_id]
	order by 2 desc
END

EXEC cusromers_by_total_spend 

---Get top 5 sellers by revenue in a given time period
alter procedure sellers_by_revenue
@start_date datetime,
@end_date datetime
AS
BEGIN
	select top(5) oi.seller_id, SUM(oi.price) as total_revenue, [order_purchase_timestamp]
	from [dbo].[olist_orders_dataset] o join [dbo].[olist_order_items_dataset] oi on o.order_id = oi.order_id
	where [order_purchase_timestamp] between @start_date and @end_date
	group by oi.seller_id, [order_purchase_timestamp]
	order by 2 desc
END

EXEC sellers_by_revenue 
    @start_date = '2018-01-01', 
    @end_date = '2018-12-31';

