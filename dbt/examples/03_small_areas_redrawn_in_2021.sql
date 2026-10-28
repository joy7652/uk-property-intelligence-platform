-- How many small areas with sales in 2019 recorded no crime that year?
--
-- Small areas redrawn in 2021 took new codes. Sales are attributed to the 2021 code for
-- every year back to 1995, while crime stayed under the 2011 code until the 2021-01 to
-- 2023-06 transition moved it across. Before 2021 a new code therefore has sales and no
-- crime row. Reading a missing row as zero is right for a code that never changed. For a
-- new code before 2021 it reports no crime in an area whose crime sits under its old code.
--
-- Results measured 28-10-2026.


-- Against Gold: crime summed to the year first and a missing row read as zero, with every
-- code kept.
with crime_2019 as (
    select lsoa_code, sum(crime_count_excl_asb) as crimes
    from uk_property_intel.gold.fact_lsoa_month_crime_total
    where year(month_start_date) = 2019
    group by lsoa_code
)

select
    count(*) as small_areas_with_sales,
    count_if(coalesce(crime_2019.crimes, 0) = 0) as recorded_no_crime
from uk_property_intel.gold.fact_lsoa_year_price as price
left join crime_2019
    on price.lsoa_code = crime_2019.lsoa_code
where price.year_start_date = date'2019-01-01';

-- small_areas_with_sales  recorded_no_crime
--                 35,636              2,023


-- Against the mart, which keeps only the codes common to both boundary sets.
select
    count(*) as small_areas_with_sales,
    count_if(crime_count_excl_asb = 0) as recorded_no_crime
from uk_property_intel.marts.mart_lsoa_year_crime_price
where year_start_date = date'2019-01-01';

-- small_areas_with_sales  recorded_no_crime
--                 33,617                  4


-- The Gold result split by boundary vintage.
with crime_2019 as (
    select lsoa_code, sum(crime_count_excl_asb) as crimes
    from uk_property_intel.gold.fact_lsoa_month_crime_total
    where year(month_start_date) = 2019
    group by lsoa_code
)

select
    lsoa.boundary_vintage,
    count(*) as small_areas_with_sales,
    count_if(coalesce(crime_2019.crimes, 0) = 0) as recorded_no_crime
from uk_property_intel.gold.fact_lsoa_year_price as price
inner join uk_property_intel.gold.dim_lsoa as lsoa
    on price.lsoa_code = lsoa.lsoa_code
left join crime_2019
    on price.lsoa_code = crime_2019.lsoa_code
where price.year_start_date = date'2019-01-01'
group by lsoa.boundary_vintage
order by lsoa.boundary_vintage;

-- boundary_vintage  small_areas_with_sales  recorded_no_crime
-- both                              33,617                  4
-- only_2021                          2,019              2,019
