
{{ config(materialized='table', alias = 'jaffle_shop_variables') }}
select id,first_name,last_name from {{ ref('raw_customers') }}
where id = 3
and first_name = {{ var('my_variable') }}