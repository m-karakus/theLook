{{
    config(
        materialized='table',
        tags=['reporting']
    )
}}

-- Kategori bazli satis ozeti

with order_items as (
    select * from {{ ref('stg_order_items') }}
    where status not in ('Cancelled', 'Returned')
),

products as (
    select * from {{ ref('stg_products') }}
)

select
    p.category,
    p.department,
    p.brand,
    count(distinct oi.order_id) as total_orders,
    count(oi.order_item_id) as total_items_sold,
    sum(oi.sale_price) as total_revenue,
    avg(oi.sale_price) as avg_sale_price,
    avg(p.cost) as avg_product_cost,
    sum(oi.sale_price) - sum(p.cost) as total_profit,
    case
        when sum(oi.sale_price) > 0
        then (sum(oi.sale_price) - sum(p.cost)) / sum(oi.sale_price) * 100
        else 0
    end as profit_margin_pct
from order_items oi
inner join products p on oi.product_id = p.product_id
group by
    p.category,
    p.department,
    p.brand
