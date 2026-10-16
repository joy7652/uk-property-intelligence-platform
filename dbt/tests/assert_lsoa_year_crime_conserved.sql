-- The annual crime total loses and gains nothing against the monthly fact, over the
-- small areas and years the mart keeps.
with mart as (
    select lsoa_code, year_start_date, crime_count_excl_asb
    from {{ ref('mart_lsoa_year_crime_price') }}
),

monthly as (
    select
        lsoa_code,
        trunc(month_start_date, 'YEAR') as year_start_date,
        crime_count_excl_asb
    from {{ source('gold', 'fact_lsoa_month_crime_total') }}
),

kept as (
    select monthly.crime_count_excl_asb
    from monthly
    inner join mart
        on monthly.lsoa_code = mart.lsoa_code
        and monthly.year_start_date = mart.year_start_date
)

select
    mart_total.crimes as mart_crimes,
    gold_total.crimes as gold_crimes
from (select sum(crime_count_excl_asb) as crimes from mart) as mart_total
cross join (select sum(crime_count_excl_asb) as crimes from kept) as gold_total
where not (mart_total.crimes <=> gold_total.crimes)
