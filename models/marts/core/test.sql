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
    status,
    payment_status,
    payment_method





from orders 
left join customers using (customer_id)
left join payments using (order_id)

