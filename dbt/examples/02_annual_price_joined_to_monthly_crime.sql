-- In the small areas whose code did not change in 2021, how many category A sales took
-- place in 2024, and how many crimes excluding anti-social behaviour were recorded?
--
-- Price is stored once per small area and year, crime once per small area and month.
-- Joined on the year, each price row meets up to twelve crime rows, and its sales are
-- counted once for every one of them.
--
-- The Gold query also drops 1,283 small areas: those with sales and no crime row in 2024.
-- 1,278 of them are in Greater Manchester, whose force published nothing that year. The
-- mart keeps them and reads their crime as zero, because it judges a year complete across
-- the whole release, and one missing force does not make the release short.
--
-- Results measured 28-10-2026.


-- Against Gold: the population is right and the grain is not.
select
    count(distinct price.lsoa_code) as small_areas,
    sum(price.transaction_count) as sales,
    sum(crime.crime_count_excl_asb) as crimes
from uk_property_intel.gold.fact_lsoa_year_price as price
inner join uk_property_intel.gold.dim_lsoa as lsoa
    on price.lsoa_code = lsoa.lsoa_code
inner join uk_property_intel.gold.fact_lsoa_month_crime_total as crime
    on price.lsoa_code = crime.lsoa_code
    and price.year_start_date = trunc(crime.month_start_date, 'YEAR')
where lsoa.boundary_vintage = 'both'
    and price.year_start_date = date'2024-01-01';

-- small_areas      sales     crimes
--      32,321  8,037,250  4,438,559


-- Against the mart, which sums crime to the year before it meets price.
select
    count(*) as small_areas,
    sum(transaction_count) as sales,
    sum(crime_count_excl_asb) as crimes
from uk_property_intel.marts.mart_lsoa_year_crime_price
where year_start_date = date'2024-01-01';

-- small_areas    sales     crimes
--      33,604  709,975  4,438,559
