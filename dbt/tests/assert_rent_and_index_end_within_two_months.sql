{{ config(severity='warn') }}

-- The two published panels end within two months of each other. A wider gap means one
-- publisher's release is missing, and the yield mart stops at the older of the two.
select
    rent.latest as rent_latest,
    hpi.latest as hpi_latest
from (
    select max(month_start_date) as latest
    from {{ source('gold', 'fact_area_month_rent') }}
) as rent
cross join (
    select max(month_start_date) as latest
    from {{ source('gold', 'fact_area_month_hpi') }}
) as hpi
where abs(
    (year(rent.latest) * 12 + month(rent.latest))
    - (year(hpi.latest) * 12 + month(hpi.latest))
) > 2
