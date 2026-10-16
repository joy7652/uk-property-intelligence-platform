-- Gross rental yield: twelve months of average rent over the published mix-adjusted
-- average price, for each district and month where both are published.

with districts as (
    select area_code, area_name, region_code, nation_code
    from {{ source('gold', 'dim_area') }}
    where area_level = 'district'
),

rent as (
    select area_code, month_start_date, rental_price
    from {{ source('gold', 'fact_area_month_rent') }}
    where rental_price is not null
),

hpi as (
    select area_code, month_start_date, avg_price
    from {{ source('gold', 'fact_area_month_hpi') }}
    where avg_price is not null
)

select
    districts.area_code as district_code,
    districts.area_name as district_name,
    districts.region_code,
    districts.nation_code,
    rent.month_start_date,
    rent.rental_price,
    hpi.avg_price,
    cast(100 * (12 * rent.rental_price) / hpi.avg_price as decimal(7, 4)) as gross_yield_pct
from rent
inner join hpi
    on rent.area_code = hpi.area_code
    and rent.month_start_date = hpi.month_start_date
inner join districts
    on rent.area_code = districts.area_code
