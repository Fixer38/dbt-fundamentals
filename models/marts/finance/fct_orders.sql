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
        customers.customer_id as customer_id,
        orders.order_id as order_id,
        payments.amount as amount

    from customers

    left join orders using (customer_id)
    left join payments using (orders.order_id)

)

select * from final