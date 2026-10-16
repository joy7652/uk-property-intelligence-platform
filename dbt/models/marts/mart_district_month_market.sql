-- One row per district and month. The level is fixed to district here: gold.dim_area
-- mixes six levels, and an additive measure summed across them counts one sale or
-- crime at every level it rolls up to.

with districts as (
    select area_code, area_name, region_code, nation_code
    from {{ source('gold', 'dim_area') }}
    where area_level = 'district'
),

-- A fact row enters only if it carries one of the columns this view takes from it.
hpi as (
    select
        area_code,
        month_start_date,
        avg_price,
        price_index as hpi_price_index,
        pct_change_12m as hpi_pct_change_12m
    from {{ source('gold', 'fact_area_month_hpi') }}
    where avg_price is not null
        or price_index is not null
        or pct_change_12m is not null
),

rent as (
    select
        area_code,
        month_start_date,
        rental_price,
        price_index as rent_price_index,
        pct_change_12m as rent_pct_change_12m
    from {{ source('gold', 'fact_area_month_rent') }}
    where rental_price is not null
        or price_index is not null
        or pct_change_12m is not null
),

prices as (
    select area_code, month_start_date, median_price, transaction_count
    from {{ source('gold', 'fact_area_month_price') }}
),

crime as (
    select area_code, month_start_date, crime_count_excl_asb
    from {{ source('gold', 'fact_area_month_crime_total') }}
),

cells as (
    select area_code, month_start_date from hpi
    union
    select area_code, month_start_date from rent
    union
    select area_code, month_start_date from prices
    union
    select area_code, month_start_date from crime
)

select
    districts.area_code as district_code,
    districts.area_name as district_name,
    districts.region_code,
    districts.nation_code,
    cells.month_start_date,
    hpi.avg_price,
    hpi.hpi_price_index,
    hpi.hpi_pct_change_12m,
    rent.rental_price,
    rent.rent_price_index,
    rent.rent_pct_change_12m,
    prices.median_price,
    prices.transaction_count,
    crime.crime_count_excl_asb
from cells
inner join districts
    on cells.area_code = districts.area_code
left join hpi
    on cells.area_code = hpi.area_code
    and cells.month_start_date = hpi.month_start_date
left join rent
    on cells.area_code = rent.area_code
    and cells.month_start_date = rent.month_start_date
left join prices
    on cells.area_code = prices.area_code
    and cells.month_start_date = prices.month_start_date
left join crime
    on cells.area_code = crime.area_code
    and cells.month_start_date = crime.month_start_date
