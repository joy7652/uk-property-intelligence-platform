-- How many category A sales took place in England and Wales in 2024, and how many crimes
-- excluding anti-social behaviour were recorded?
--
-- Both facts carry a row at four levels: district, region, nation and the England and
-- Wales composite. A sale or a crime in England sits in all four, and one in Wales in
-- three, since only England is divided into regions. A sum with no level filter counts
-- each of them three or four times.
--
-- Crime is what police.uk publish. Greater Manchester Police published nothing for 2024,
-- so neither crime figure counts the crime that force records.
--
-- Results measured 28-10-2026.


-- Against Gold: each fact summed as stored.
select
    (
        select sum(transaction_count)
        from uk_property_intel.gold.fact_area_month_price
        where year(month_start_date) = 2024
    ) as sales,
    (
        select sum(crime_count_excl_asb)
        from uk_property_intel.gold.fact_area_month_crime_total
        where year(month_start_date) = 2024
    ) as crimes;

--     sales      crimes
-- 3,013,965  19,225,323


-- Against the mart, which holds districts and nothing else.
select
    sum(transaction_count) as sales,
    sum(crime_count_excl_asb) as crimes
from uk_property_intel.marts.mart_district_month_market
where year(month_start_date) = 2024;

--   sales     crimes
-- 763,164  4,869,305


-- Where the Gold figure comes from. Each level holds the whole of its territory, so the
-- districts, the two nations and the composite each carry the full total and the regions
-- carry England's.
with sales as (
    select area.area_level, sum(price.transaction_count) as sales
    from uk_property_intel.gold.fact_area_month_price as price
    inner join uk_property_intel.gold.dim_area as area
        on price.area_code = area.area_code
    where year(price.month_start_date) = 2024
    group by area.area_level
),

crimes as (
    select area.area_level, sum(crime.crime_count_excl_asb) as crimes
    from uk_property_intel.gold.fact_area_month_crime_total as crime
    inner join uk_property_intel.gold.dim_area as area
        on crime.area_code = area.area_code
    where year(crime.month_start_date) = 2024
    group by area.area_level
)

select
    coalesce(sales.area_level, crimes.area_level) as area_level,
    sales.sales,
    crimes.crimes
from sales
full outer join crimes
    on sales.area_level = crimes.area_level
order by area_level;

-- area_level    sales     crimes
-- composite   763,164  4,869,305
-- district    763,164  4,869,305
-- nation      763,164  4,869,305
-- region      724,473  4,617,408
