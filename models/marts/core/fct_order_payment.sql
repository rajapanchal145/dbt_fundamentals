{{ config (
    materialized="table"
)}}

with customers as (

    select * from {{ ref('stg_customers')}}

),

orders as (
    select * from {{ ref('stg_orders') }}

),

payments as (
    select * from {{ref('stg_payments')}}
),

final as (

    select
        --ROW_NUMBER() as id,
        orders.order_id,
        payments.payment_id,
        customers.customer_id,
        orders.status,
        payments.status,
        payments.payment_method,
        payments.amount,
        orders.order_date,
        payments.created_at as payment_date
        --CURRENT_TIMESTAMP as created_at 
    from orders 
    join left customers using (customer_id)
    join left payments using (order_id)
)

select * from final