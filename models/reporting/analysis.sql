with cohort_analysis as (

    select * from {{ ref('fact_cohort_analysis')}}

),

order_payment_analysis as (
    select * from {{ ref('fact_order_payments') }}

)

select * from cohort_analysis