with payments as (

    select *
    from {{ ref("stg_stripe__payments")}}

),

customers as (

    select *
    from {{ ref("stg_jaffle_shop__customers")}}
    
),

orders as (

    select *
    from {{ ref("stg_jaffle_shop__orders")}}

),

final as (

    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce (order_payments.amount, 0) as amount

    from orders
    left join order_payments using (order_id)
)

select * from final