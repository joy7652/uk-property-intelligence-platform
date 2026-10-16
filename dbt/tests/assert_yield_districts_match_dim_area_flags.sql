-- Every district gold.dim_area flags with both a price index and a rent has a yield, and
-- no other district does. A failure names the districts on either side.
with computed as (
    select distinct district_code
    from {{ ref('mart_district_yield') }}
),

flagged as (
    select area_code as district_code
    from {{ source('gold', 'dim_area') }}
    where area_level = 'district' and has_price_index and has_rent_index
)

select
    coalesce(computed.district_code, flagged.district_code) as district_code,
    computed.district_code is not null as has_yield,
    flagged.district_code is not null as flagged_in_dim_area
from computed
full outer join flagged
    on computed.district_code = flagged.district_code
where computed.district_code is null or flagged.district_code is null
