{{
    config(
        materialized='table',
        tags=['reporting']
    )
}}

-- Musteri basina toplam gelir + siparis sayisi

with order_items as (
    select * from {{ ref('stg_order_items') }}
    where status not in ('Cancelled', 'Returned')
),

orders as (
    select * from {{ ref('stg_orders') }}
    where status not in ('Cancelled', 'Returned')
),

users as (
    select * from {{ ref('stg_users') }}
),

customer_orders as (
    select
        oi.user_id,
        count(distinct oi.order_id) as total_orders,
        count(oi.order_item_id) as total_items,
        sum(oi.sale_price) as total_revenue,
        min(oi.created_at) as first_order_at,
        max(oi.created_at) as last_order_at
    from order_items oi
    group by oi.user_id
)

select
    u.user_id,
    u.first_name,
    u.last_name,
    u.email,
    u.age,
    u.gender,
    u.city,
    u.state,
    u.country,
    u.traffic_source,
    coalesce(co.total_orders, 0) as total_orders,
    coalesce(co.total_items, 0) as total_items,
    coalesce(co.total_revenue, 0) as total_revenue,
    case
        when co.total_orders > 0
        then co.total_revenue / co.total_orders
        else 0
    end as avg_order_value,
    co.first_order_at,
    co.last_order_at
from users u
left join customer_orders co on u.user_id = co.user_id
