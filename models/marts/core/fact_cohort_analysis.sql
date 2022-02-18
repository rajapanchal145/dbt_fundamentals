{{
    config(
        materialized='table'
    )
}}

with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (
    select * from {{ ref('stg_orders') }}

),

payments as (
    select * from {{ref('stg_payments')}}
),

avg_day_orders as
(
select customer_id,(-1*(order_date-lag(order_date,-1,order_date) 
over( partition by customer_id order by customer_id,order_date asc))) as ado_c 
from orders 
order by order_date
)

select
    row_number() over (order by customer_id) as ID,
    customer_id,
    min(order_date) as first_order_date,
    max(order_date) as last_order_date,
    count(order_id) as total_orders,
    sum(amount) as payment_amount,
    avg(ado_c) as average_days_between_orders,
    current_timestamp() as created_at
from orders
left join customers using (customer_id)
left join payments using (order_id)
left join avg_day_orders using (customer_id)
group by customer_id;


