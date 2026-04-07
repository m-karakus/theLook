-- Every order_id in order_items must exist in orders
-- This test fails if any orphan order_item is found

SELECT
    oi.id,
    oi.order_id
FROM {{ ref('stg_order_items') }} oi
LEFT JOIN {{ ref('stg_orders') }} o ON oi.order_id = o.order_id
WHERE o.order_id IS NULL
