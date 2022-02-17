with payments as (
    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        -- amount is stored in cents, convert it to dollars
        amount / 100 as amount,
        created as payment_date

    from {{ source('stripe', 'payment') }} 
)


select * from payments
