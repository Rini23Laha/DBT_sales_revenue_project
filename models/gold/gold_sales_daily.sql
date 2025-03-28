
select
o.order_date,
p.product_name,
p.category,
p.vendor,
u.city,
u.state,
u.sales_channel,
sum(o.order_amount) as total_revenue
from 
{{ref ('silver_orders')}} o
LEFT JOIN 
{{ref ('silver_products')}} p
on o.product_id = p.id
Left Join 
{{ref ('silver_users')}} u
on o.user_id = u.id
group by all