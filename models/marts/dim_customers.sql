-- {%- set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%}

{{ config(materialized='table')}}


with customers as (

    select

     {{ dbt_utils.generate_surrogate_key(['customer_id','first_name']) }} as id,
     customer_id,
     first_name,
     last_name
     

from {{ ref('stg_customers') }}
order by customer_id

),

orders as (

    select * from {{ref('stg_orders')}}
),

pay as (

    select * from {{ref('stg_payments')}}
),

customer_orders as (

    select
        
        orders.order_id,
        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.order_id) as number_of_orders
        
        -- sum(case when payment_method in('credit_card','coupon','gift_card')  then amount_usd else 0 end) as value

    from pay join orders on orders.order_id = pay.order_id  
    group by 1
            
),



final as (

    select
        customers.id,
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.order_id,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        customer_orders.number_of_orders
        -- customer_orders.value

        
    
        from customers join customer_orders on customers.customer_id = customer_orders.order_id
        
       
)

select * from final

