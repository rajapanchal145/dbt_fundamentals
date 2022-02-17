with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (
    select * from {{ ref('stg_orders') }}

),

payments as (
    select * from {{ref('stg_payments')}}
)

select
    order_id,
    payment_id,
    customer_id,
    --order_status,
    payment_method,
    --payment_amount,
    --payment_status,
    order_date
    --payment_date

from orders 
left join customers using (customer_id)
left join payments using (order_id)

