-- Annual crime beside annual price, by small area. Two populations are refused: codes
-- that changed at the 2021 boundary revision, whose crime splits across two codes during
-- the transition, and years for which the crime table lacks a month.

with small_areas as (
    select lsoa_code, lsoa_name, district_code, district_assignment
    from {{ source('gold', 'dim_lsoa') }}
    where boundary_vintage = 'both'
),

monthly_crime as (
    select
        lsoa_code,
        trunc(month_start_date, 'YEAR') as year_start_date,
        month_start_date,
        crime_count_excl_asb
    from {{ source('gold', 'fact_lsoa_month_crime_total') }}
),

complete_years as (
    select year_start_date
    from monthly_crime
    group by year_start_date
    having count(distinct month_start_date) = 12
),

annual_crime as (
    select
        lsoa_code,
        year_start_date,
        cast(sum(crime_count_excl_asb) as int) as crime_count_excl_asb
    from monthly_crime
    group by lsoa_code, year_start_date
),

prices as (
    select lsoa_code, year_start_date, median_price, transaction_count
    from {{ source('gold', 'fact_lsoa_year_price') }}
)

-- The crime facts carry a row only where something was recorded, so a small area with
-- sales and no crime row in a complete year recorded none.
select
    small_areas.lsoa_code,
    small_areas.lsoa_name,
    small_areas.district_code,
    small_areas.district_assignment,
    prices.year_start_date,
    prices.median_price,
    prices.transaction_count,
    coalesce(annual_crime.crime_count_excl_asb, 0) as crime_count_excl_asb
from prices
inner join small_areas
    on prices.lsoa_code = small_areas.lsoa_code
inner join complete_years
    on prices.year_start_date = complete_years.year_start_date
left join annual_crime
    on prices.lsoa_code = annual_crime.lsoa_code
    and prices.year_start_date = annual_crime.year_start_date
