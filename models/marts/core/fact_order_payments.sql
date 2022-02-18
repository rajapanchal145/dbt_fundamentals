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
)

select
    row_number() over (order by order_id) as ID,
    order_id,
    payment_id,
    customer_id,
    orders.status as order_status,
    payment_method,
    amount as payment_amount,
    payments.status as payment_status,
    order_date,
    created_at as payment_date,
    current_timestamp() as created_at

from orders 
left join customers using (customer_id)
left join payments using (order_id)

