{{
    config(
        materialized='table',
        tags=['reporting']
    )
}}

-- Gunluk ortalama siparis degeri (AOV)

with order_items as (
    select * from {{ ref('stg_order_items') }}
    where status not in ('Cancelled', 'Returned')
),

daily_orders as (
    select
        toDate(created_at) as order_date,
        order_id,
        sum(sale_price) as order_total
    from order_items
    group by
        toDate(created_at),
        order_id
)

select
    order_date,
    count(distinct order_id) as total_orders,
    sum(order_total) as total_revenue,
    avg(order_total) as avg_order_value,
    min(order_total) as min_order_value,
    max(order_total) as max_order_value
from daily_orders
group by order_date
order by order_date
