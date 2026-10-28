-- Did recorded crime rise or fall in the latest year?
--
-- The crime table grows a month at a time, so the year in progress holds fewer than twelve
-- months and its total reads as a fall.
--
-- Results measured 28-10-2026.


-- Against Gold: annual totals over whichever months are loaded.
with annual as (
    select year(month_start_date) as calendar_year, sum(crime_count_excl_asb) as crimes
    from uk_property_intel.gold.fact_lsoa_month_crime_total
    group by year(month_start_date)
)

select
    calendar_year,
    crimes,
    round(100 * (crimes / lag(crimes) over (order by calendar_year) - 1), 1) as change_pct
from annual
order by calendar_year desc
limit 3;

-- calendar_year     crimes  change_pct
--          2026  2,329,721       -51.1
--          2025  4,765,064        -2.1
--          2024  4,869,305        -1.7


-- Against the mart, which holds a year only once all twelve months are loaded. It covers
-- fewer small areas, so its totals are lower and the change is the figure to compare.
with annual as (
    select year(year_start_date) as calendar_year, sum(crime_count_excl_asb) as crimes
    from uk_property_intel.marts.mart_lsoa_year_crime_price
    group by year(year_start_date)
)

select
    calendar_year,
    crimes,
    round(100 * (crimes / lag(crimes) over (order by calendar_year) - 1), 1) as change_pct
from annual
order by calendar_year desc
limit 3;

-- calendar_year     crimes  change_pct
--          2025  4,349,416        -2.0
--          2024  4,438,559        -1.9
--          2023  4,524,254        -1.3


-- Months loaded per year.
select
    year(month_start_date) as calendar_year,
    count(distinct month_start_date) as months
from uk_property_intel.gold.fact_lsoa_month_crime_total
group by year(month_start_date)
order by calendar_year desc
limit 3;

-- calendar_year  months
--          2026       6
--          2025      12
--          2024      12
