-- A count is additive, so crime summed over districts equals crime summed over small
-- areas. A level filter that slipped, or a join that fanned out, breaks the equality.
select
    districts.crimes as district_crimes,
    small_areas.crimes as small_area_crimes
from (
    select sum(crime_count_excl_asb) as crimes
    from {{ ref('mart_district_month_market') }}
) as districts
cross join (
    select sum(crime_count_excl_asb) as crimes
    from {{ source('gold', 'fact_lsoa_month_crime_total') }}
) as small_areas
where not (districts.crimes <=> small_areas.crimes)
