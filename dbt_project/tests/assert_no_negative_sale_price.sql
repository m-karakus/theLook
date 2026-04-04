-- Negative sale prices should not exist in order items
-- This test fails if any row is returned

SELECT
    id,
    order_id,
    sale_price
FROM {{ ref('stg_order_items') }}
WHERE sale_price < 0
