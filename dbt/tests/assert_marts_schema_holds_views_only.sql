-- depends_on: {{ ref('mart_district_month_market') }}
-- depends_on: {{ ref('mart_district_yield') }}
-- depends_on: {{ ref('mart_lsoa_year_crime_price') }}

-- The schema has no managed location. A table built in it would be written to the
-- catalog's own location, which is kept empty.
select table_name, table_type
from {{ target.database }}.information_schema.tables
where table_schema = '{{ target.schema }}'
    and table_type <> 'VIEW'
