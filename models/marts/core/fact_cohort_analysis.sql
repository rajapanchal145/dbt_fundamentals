with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (
    select * from {{ ref('stg_orders') }}

),

payments as (
    select * from {{ref('stg_payments')}}
),

ado as
(
select customer_id,(-1*(order_date-lag(order_date,-1,order_date) 
over( partition by customer_id order by customer_id,order_date asc))) as ado_c 
from orders 
order by order_date
)

select
    customer_id,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    count(order_id) as total_orders,
    --sum(payment_amount),
    avg(ado_c)
from orders
left join customers using (customer_id)
left join payments using (order_id)
left join ado using (customer_id)
group by customer_id;


