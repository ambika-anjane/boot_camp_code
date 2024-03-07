-- {%- set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%}

-- {{ config(materialized='table')}}
 {{ config(materialized='incremental'

 )}}






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

    select
 id as order_id, 
 order_date,
 user_id,
 _etl_loaded_at from {{ ref('raw_orders') }}


),

pay as (

    select * from {{ref('stg_payments')}}
),

customer_orders as (

    select
        
        orders.order_id,
        orders.user_id,
        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.order_id) as number_of_orders
        
        -- sum(case when payment_method in('credit_card','coupon','gift_card')  then amount_usd else 0 end) as value

    from pay join orders on orders.order_id = pay.order_id  
    group by 1,2

      
            
),



final as (

    select
        customers.id,
        customer_orders.order_id,
        customer_orders.user_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        customer_orders.number_of_orders,
        orders._etl_loaded_at

        
    
        from customers join customer_orders on customers.customer_id = customer_orders.order_id
        inner join orders on customers.customer_id = orders.order_id
     {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where orders._etl_loaded_at >= (select max(_etl_loaded_at) from {{ this }})
{% endif %}
        
       ''
)

select * from final

