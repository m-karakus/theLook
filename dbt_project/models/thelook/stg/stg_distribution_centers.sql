with source as (
    select * from {{ source('raw', 'raw_bq__distribution_centers') }}
)

select
    id as distribution_center_id,
    name as distribution_center_name,
    latitude,
    longitude
from source
