# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Gold dimension tables
# MAGIC
# MAGIC Declared DDL. Tables are created empty and filled by `INSERT OVERWRITE`, so the schema is
# MAGIC fixed here and never inferred from a write.
# MAGIC
# MAGIC `CREATE TABLE IF NOT EXISTS` does not alter an existing table, so changing a column needs an
# MAGIC explicit `ALTER TABLE` or a drop. The alternative, `CREATE OR REPLACE`, would discard Gold on
# MAGIC any accidental rerun.
# MAGIC
# MAGIC Check constraints are dropped before being added, which makes the notebook re-runnable.
# MAGIC Adding a constraint validates every existing row: free while the tables are empty, expensive
# MAGIC once they are not. Run this before the first load.
# MAGIC
# MAGIC Primary and foreign keys are informational. Unity Catalog does not enforce them; they
# MAGIC describe the model to the optimiser and to Power BI. A foreign key needs its parent's primary
# MAGIC key to already exist, so the order of the cells below matters.

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_date
# MAGIC
# MAGIC One row per day from 1973-01-01, where the base rate series starts, to the end of the current
# MAGIC year. Roughly 19,700 rows.
# MAGIC
# MAGIC Monthly facts key on the first of the month, which is a row here, so they join on `date_key`
# MAGIC directly. Selecting a month in a report brings every day of that month into scope, filters the
# MAGIC monthly fact to its one row, and leaves the daily base rate open to be averaged, taken at
# MAGIC either end, or read as a step series. No month-level rate is stored, because those answers
# MAGIC diverge in months like March 2020 and the right one depends on the question.
# MAGIC
# MAGIC `base_rate_pct` is `NOT NULL` as an assertion. The four rate regimes form one unbroken chain,
# MAGIC each closing the day before the next begins. A null would mean the source has changed shape,
# MAGIC and the load should fail instead of filling the gap.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.dim_date (
# MAGIC   date_key            DATE          NOT NULL COMMENT 'Calendar day. Grain of this table.',
# MAGIC   calendar_year       INT           NOT NULL,
# MAGIC   calendar_quarter    TINYINT       NOT NULL,
# MAGIC   calendar_month      TINYINT       NOT NULL,
# MAGIC   day_of_month        TINYINT       NOT NULL,
# MAGIC   day_of_week         TINYINT       NOT NULL COMMENT 'ISO weekday, 1 Monday to 7 Sunday.',
# MAGIC   month_name          STRING        NOT NULL,
# MAGIC   day_name            STRING        NOT NULL,
# MAGIC   year_month          INT           NOT NULL COMMENT 'Year and month as an integer, 202608. Sorts chronologically.',
# MAGIC   month_start_date    DATE          NOT NULL,
# MAGIC   quarter_start_date  DATE          NOT NULL,
# MAGIC   is_month_end        BOOLEAN       NOT NULL,
# MAGIC   is_weekend          BOOLEAN       NOT NULL,
# MAGIC   base_rate_pct       DECIMAL(6,4)  NOT NULL COMMENT 'Bank of England headline rate in force on this day, percent.',
# MAGIC   base_rate_type      STRING        NOT NULL COMMENT 'Instrument name on this day: Minimum Lending Rate, Minimum Band 1 Dealing Rate, Repo Rate, Official Bank Rate.',
# MAGIC   CONSTRAINT dim_date_pk PRIMARY KEY (date_key)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Daily calendar from 1973, carrying the Bank of England headline rate as an attribute.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.dim_date DROP CONSTRAINT IF EXISTS dim_date_month_range;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_date ADD CONSTRAINT dim_date_month_range
# MAGIC   CHECK (calendar_month BETWEEN 1 AND 12 AND calendar_quarter BETWEEN 1 AND 4 AND day_of_week BETWEEN 1 AND 7);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_date DROP CONSTRAINT IF EXISTS dim_date_year_month_derived;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_date ADD CONSTRAINT dim_date_year_month_derived
# MAGIC   CHECK (year_month = calendar_year * 100 + calendar_month);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_date DROP CONSTRAINT IF EXISTS dim_date_starts_align;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_date ADD CONSTRAINT dim_date_starts_align
# MAGIC   CHECK (month_start_date = trunc(date_key, 'MM') AND quarter_start_date = trunc(date_key, 'QUARTER'));
# MAGIC
# MAGIC -- Upper bound sits above the 17 percent peak of 1979.
# MAGIC ALTER TABLE uk_property_intel.gold.dim_date DROP CONSTRAINT IF EXISTS dim_date_rate_positive;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_date ADD CONSTRAINT dim_date_rate_positive
# MAGIC   CHECK (base_rate_pct > 0 AND base_rate_pct < 25);

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_area
# MAGIC
# MAGIC Every published area the platform reports on, at any level. 432 rows: 424 carrying a GSS code
# MAGIC and 8 Northern Irish rental market areas that ONS publishes without one.
# MAGIC
# MAGIC Levels are not a single hierarchy. Districts roll up to region and then to nation. County
# MAGIC areas carry their own published index and no children, because the only district-to-county
# MAGIC membership available uses ceremonial counties on a code series whose values collide with the
# MAGIC metropolitan county codes the index uses. Rental market areas are a separate geography that
# MAGIC nests inside a nation and matches nothing below it.
# MAGIC
# MAGIC Ancestry is flattened rather than walked. `region_code` and `nation_code` sit beside
# MAGIC `parent_area_code`, so a row states every level it belongs to and a figure at any level
# MAGIC counts each area once whether or not anything sits below it. An area reaching only nation
# MAGIC level still contributes there. `region_code` is null for the 65 districts in Wales,
# MAGIC Scotland and Northern Ireland, since only England is divided into regions.
# MAGIC
# MAGIC Names come from the postcode directory at district level, from the house price index above
# MAGIC it, and from the rent series for rental market areas. Seventeen codes are named differently by
# MAGIC different publishers, so `name_source` records which rule applied.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.dim_area (
# MAGIC   area_code         STRING   NOT NULL COMMENT 'GSS code, or a project-assigned code for the uncoded rental market areas. Grain of this table.',
# MAGIC   area_name         STRING   NOT NULL COMMENT 'Whitespace trimmed on load.',
# MAGIC   area_level        STRING   NOT NULL COMMENT 'district, county, region, nation, composite, or rental_market_area.',
# MAGIC   parent_area_code  STRING            COMMENT 'Immediate parent. Region for an English district, nation for a district elsewhere and for a region or rental market area, null for nations, composites and counties.',
# MAGIC   region_code       STRING            COMMENT 'Region this area sits in, or its own code for a region. Null outside England, the only nation divided into regions, and null for counties, composites and rental market areas.',
# MAGIC   nation_code       STRING            COMMENT 'Nation this area sits in. Null for composites spanning more than one nation.',
# MAGIC   country_name      STRING            COMMENT 'England, Wales, Scotland or Northern Ireland. Null for composites.',
# MAGIC   code_source       STRING   NOT NULL COMMENT 'published where the code is a GSS code, derived where this project assigned one.',
# MAGIC   name_source       STRING   NOT NULL COMMENT 'Publisher the name was taken from: postcode_directory, house_price_index, or rent_series.',
# MAGIC   has_price_index   BOOLEAN  NOT NULL COMMENT 'True where the house price index publishes this area.',
# MAGIC   has_rent_index    BOOLEAN  NOT NULL COMMENT 'True where the private rent series publishes this area.',
# MAGIC   has_postcodes     BOOLEAN  NOT NULL COMMENT 'True where the postcode directory assigns postcodes to this area. Required for any transaction or crime measure.',
# MAGIC   CONSTRAINT dim_area_pk PRIMARY KEY (area_code)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Published areas at every level the sources report on, conforming through district_code.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area DROP CONSTRAINT IF EXISTS dim_area_level_values;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area ADD CONSTRAINT dim_area_level_values
# MAGIC   CHECK (area_level IN ('district', 'county', 'region', 'nation', 'composite', 'rental_market_area'));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area DROP CONSTRAINT IF EXISTS dim_area_code_source_values;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area ADD CONSTRAINT dim_area_code_source_values
# MAGIC   CHECK (code_source IN ('published', 'derived'));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area DROP CONSTRAINT IF EXISTS dim_area_name_source_values;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area ADD CONSTRAINT dim_area_name_source_values
# MAGIC   CHECK (name_source IN ('postcode_directory', 'house_price_index', 'rent_series'));
# MAGIC
# MAGIC -- A GSS code is one letter then eight digits. A project-assigned code must not look like one.
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area DROP CONSTRAINT IF EXISTS dim_area_code_shape;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area ADD CONSTRAINT dim_area_code_shape
# MAGIC   CHECK (
# MAGIC     (code_source = 'published' AND area_code RLIKE '^[A-Z][0-9]{8}$')
# MAGIC     OR (code_source = 'derived' AND area_code RLIKE '^BRMA_NI_[A-Z_]+$')
# MAGIC   );
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area DROP CONSTRAINT IF EXISTS dim_area_parent_not_self;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area ADD CONSTRAINT dim_area_parent_not_self
# MAGIC   CHECK (parent_area_code IS NULL OR parent_area_code <> area_code);
# MAGIC
# MAGIC -- Only a rental market area is published with rent and no postcodes.
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area DROP CONSTRAINT IF EXISTS dim_area_rent_needs_postcodes;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area ADD CONSTRAINT dim_area_rent_needs_postcodes
# MAGIC   CHECK (has_postcodes OR NOT has_rent_index OR area_level = 'rental_market_area');
# MAGIC
# MAGIC -- A district's region is resolved by matching the postcode directory's region name to
# MAGIC -- the price index's, and the index publishes E12000005 as 'West Midlands Region' to
# MAGIC -- keep it apart from the metropolitan county E11000005 of the same name. A match
# MAGIC -- landing on the county is the failure worth constraining. Only an E12 code is a region.
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area DROP CONSTRAINT IF EXISTS dim_area_region_is_a_region;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area ADD CONSTRAINT dim_area_region_is_a_region
# MAGIC   CHECK (region_code IS NULL OR region_code RLIKE '^E12[0-9]{6}$');
# MAGIC
# MAGIC -- A region is its own region, and no level below district carries one.
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area DROP CONSTRAINT IF EXISTS dim_area_region_level;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_area ADD CONSTRAINT dim_area_region_level
# MAGIC   CHECK (
# MAGIC     region_code IS NULL
# MAGIC     OR area_level = 'district'
# MAGIC     OR (area_level = 'region' AND region_code = area_code)
# MAGIC   );

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_lsoa
# MAGIC
# MAGIC Lower layer super output areas in England and Wales, 36,778 rows: every area in those two
# MAGIC nations that the crime source publishes or a transaction resolves to.
# MAGIC
# MAGIC The nation qualifier is not decoration. A postcode unit lying across the Anglo-Scottish
# MAGIC border is assigned whole to one district, so a small number of England and Wales
# MAGIC transactions resolve to a Scottish data zone. Those are excluded, which is why this table
# MAGIC holds three fewer rows than the postcode directory alone would suggest.
# MAGIC
# MAGIC 36,696 of the 36,778 areas sit in exactly one district. The 82 that straddle
# MAGIC take the district holding most of their postcodes, and `district_assignment` marks them so the
# MAGIC estimate stays separable from the exact ones. Every one of the 82 holds at least 92 percent
# MAGIC of its postcodes in the district it takes, so no assignment comes down to a tie-break.
# MAGIC
# MAGIC `boundary_vintage` records which census boundary set the code belongs to. Codes present in
# MAGIC both describe an area that did not change. Codes exclusive to 2011 carry crime and no price,
# MAGIC because transactions are attributed to 2021 codes only.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.dim_lsoa (
# MAGIC   lsoa_code            STRING         NOT NULL COMMENT 'LSOA code as published. Grain of this table.',
# MAGIC   lsoa_name            STRING                  COMMENT 'Area name for the vintage this code belongs to.',
# MAGIC   district_code        STRING         NOT NULL COMMENT 'Local authority district this area is counted under.',
# MAGIC   district_assignment  STRING         NOT NULL COMMENT 'exact where all postcodes fall in one district, majority where the area straddles.',
# MAGIC   majority_share       DECIMAL(5,4)   NOT NULL COMMENT 'Share of postcodes in the assigned district. 1.0000 where exact.',
# MAGIC   boundary_vintage     STRING         NOT NULL COMMENT 'both, only_2011, or only_2021.',
# MAGIC   nation_code          STRING         NOT NULL COMMENT 'E92000001 or W92000004.',
# MAGIC   has_crime            BOOLEAN        NOT NULL COMMENT 'True where the crime source publishes this code.',
# MAGIC   has_price            BOOLEAN        NOT NULL COMMENT 'True where at least one transaction resolves to this code.',
# MAGIC   CONSTRAINT dim_lsoa_pk PRIMARY KEY (lsoa_code),
# MAGIC   CONSTRAINT dim_lsoa_district_fk FOREIGN KEY (district_code) REFERENCES uk_property_intel.gold.dim_area
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Small areas in England and Wales, conforming to dim_area through district_code.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa DROP CONSTRAINT IF EXISTS dim_lsoa_code_shape;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa ADD CONSTRAINT dim_lsoa_code_shape
# MAGIC   CHECK (lsoa_code RLIKE '^[EW]01[0-9]{6}$');
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa DROP CONSTRAINT IF EXISTS dim_lsoa_assignment_values;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa ADD CONSTRAINT dim_lsoa_assignment_values
# MAGIC   CHECK (district_assignment IN ('exact', 'majority'));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa DROP CONSTRAINT IF EXISTS dim_lsoa_vintage_values;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa ADD CONSTRAINT dim_lsoa_vintage_values
# MAGIC   CHECK (boundary_vintage IN ('both', 'only_2011', 'only_2021'));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa DROP CONSTRAINT IF EXISTS dim_lsoa_share_matches_assignment;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa ADD CONSTRAINT dim_lsoa_share_matches_assignment
# MAGIC   CHECK (
# MAGIC     (district_assignment = 'exact' AND majority_share = 1.0)
# MAGIC     OR (district_assignment = 'majority' AND majority_share > 0.5 AND majority_share < 1.0)
# MAGIC   );
# MAGIC
# MAGIC -- Transactions are attributed to 2021 codes, so a 2011-exclusive code cannot carry a price.
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa DROP CONSTRAINT IF EXISTS dim_lsoa_vintage_price;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa ADD CONSTRAINT dim_lsoa_vintage_price
# MAGIC   CHECK (boundary_vintage <> 'only_2011' OR has_price = false);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa DROP CONSTRAINT IF EXISTS dim_lsoa_nation_values;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_lsoa ADD CONSTRAINT dim_lsoa_nation_values
# MAGIC   CHECK (nation_code IN ('E92000001', 'W92000004'));

# COMMAND ----------

# MAGIC %md
# MAGIC ## dim_crime_type
# MAGIC
# MAGIC Sixteen types across three vocabulary eras. The vocabulary changed at 2011-09 and again at
# MAGIC 2013-05, both times by splitting an existing type into new ones that sum back to it. An
# MAGIC all-types total is therefore comparable across the whole series while an individual type
# MAGIC series is not. `predecessor_crime_type` records what a type was split out of, which any
# MAGIC cross-era reconstruction needs.
# MAGIC
# MAGIC Anti-social behaviour is held out of the crime fact entirely. `is_anti_social_behaviour`
# MAGIC documents that absence; it is not the mechanism producing it.
# MAGIC
# MAGIC The self-reference on `predecessor_crime_type` is not declared as a foreign key. It is checked
# MAGIC at load instead.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.dim_crime_type (
# MAGIC   crime_type                STRING   NOT NULL COMMENT 'Crime type as published. Grain of this table.',
# MAGIC   first_published_month     DATE     NOT NULL COMMENT 'First month this type appears in the source.',
# MAGIC   last_published_month      DATE     NOT NULL COMMENT 'Last month this type appears in the source.',
# MAGIC   is_current                BOOLEAN  NOT NULL COMMENT 'True where the type is still published in the latest month.',
# MAGIC   vocabulary_era            TINYINT  NOT NULL COMMENT '1 from 2010-12, 2 from 2011-09, 3 from 2013-05.',
# MAGIC   predecessor_crime_type    STRING            COMMENT 'Type this one was split out of. Null for types present from the start.',
# MAGIC   is_anti_social_behaviour  BOOLEAN  NOT NULL COMMENT 'True for anti-social behaviour, which is excluded from every crime total.',
# MAGIC   CONSTRAINT dim_crime_type_pk PRIMARY KEY (crime_type)
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Crime types with the era boundaries and split lineage that make them comparable.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.dim_crime_type DROP CONSTRAINT IF EXISTS dim_crime_type_era_values;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_crime_type ADD CONSTRAINT dim_crime_type_era_values
# MAGIC   CHECK (vocabulary_era IN (1, 2, 3));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_crime_type DROP CONSTRAINT IF EXISTS dim_crime_type_month_order;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_crime_type ADD CONSTRAINT dim_crime_type_month_order
# MAGIC   CHECK (last_published_month >= first_published_month);
# MAGIC
# MAGIC -- An era 1 type predates both splits, so nothing precedes it.
# MAGIC ALTER TABLE uk_property_intel.gold.dim_crime_type DROP CONSTRAINT IF EXISTS dim_crime_type_predecessor_era;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_crime_type ADD CONSTRAINT dim_crime_type_predecessor_era
# MAGIC   CHECK (vocabulary_era > 1 OR predecessor_crime_type IS NULL);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.dim_crime_type DROP CONSTRAINT IF EXISTS dim_crime_type_not_own_predecessor;
# MAGIC ALTER TABLE uk_property_intel.gold.dim_crime_type ADD CONSTRAINT dim_crime_type_not_own_predecessor
# MAGIC   CHECK (predecessor_crime_type IS NULL OR predecessor_crime_type <> crime_type);

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold fact tables
# MAGIC
# MAGIC Nine facts across two grains. Facts below district key on `dim_lsoa`; everything else keys on
# MAGIC `dim_area`, and the two meet at `district_code`.
# MAGIC
# MAGIC Every fact keys on time through `dim_date`. A monthly fact stores the first of its month and
# MAGIC an annual fact the first of its year, both of which are rows in the daily calendar, so all of
# MAGIC them join on `date_key` without a second date dimension.
# MAGIC
# MAGIC Composite primary keys declare the grain. They are informational and unenforced, so the load
# MAGIC still has to check them.
# MAGIC
# MAGIC The large facts use liquid clustering. Predictive Optimization is inherited from the
# MAGIC metastore, so `OPTIMIZE` and `VACUUM` run without being scheduled.
# MAGIC
# MAGIC Measures carry only what the four screens ask of them. The house price index publishes 56
# MAGIC columns and the rent series 40; the rest stay in Silver, where they remain queryable.

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_area_month_hpi
# MAGIC
# MAGIC House price index, roughly 147,000 rows. Coverage starts 1995-01 in England and Wales,
# MAGIC 2004-01 in Scotland and 2005-01 in Northern Ireland, so the row count varies by area.
# MAGIC
# MAGIC Index values are mix-adjusted and published at every level, so a region is read from its own
# MAGIC row and never summed from its districts.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.fact_area_month_hpi (
# MAGIC   area_code                        STRING         NOT NULL,
# MAGIC   month_start_date                 DATE           NOT NULL COMMENT 'First of the month. Joins dim_date.date_key.',
# MAGIC   avg_price                        DECIMAL(18,6)           COMMENT 'Mix-adjusted average price, pounds.',
# MAGIC   avg_price_seasonally_adjusted    DECIMAL(18,6),
# MAGIC   price_index                      DECIMAL(18,6)           COMMENT 'Index, 100 at January 2015.',
# MAGIC   price_index_seasonally_adjusted  DECIMAL(18,6),
# MAGIC   pct_change_1m                    DECIMAL(18,6),
# MAGIC   pct_change_12m                   DECIMAL(18,6),
# MAGIC   sales_volume                     INT                     COMMENT 'Null in recent months until registrations complete.',
# MAGIC   detached_price                   DECIMAL(18,6),
# MAGIC   semi_detached_price              DECIMAL(18,6),
# MAGIC   terraced_price                   DECIMAL(18,6),
# MAGIC   flat_price                       DECIMAL(18,6),
# MAGIC   CONSTRAINT fact_area_month_hpi_pk PRIMARY KEY (area_code, month_start_date),
# MAGIC   CONSTRAINT fact_area_month_hpi_area_fk FOREIGN KEY (area_code) REFERENCES uk_property_intel.gold.dim_area,
# MAGIC   CONSTRAINT fact_area_month_hpi_date_fk FOREIGN KEY (month_start_date) REFERENCES uk_property_intel.gold.dim_date
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Monthly house price index by published area.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_hpi DROP CONSTRAINT IF EXISTS fact_area_month_hpi_month_start;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_hpi ADD CONSTRAINT fact_area_month_hpi_month_start
# MAGIC   CHECK (month_start_date = trunc(month_start_date, 'MM'));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_hpi DROP CONSTRAINT IF EXISTS fact_area_month_hpi_prices_positive;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_hpi ADD CONSTRAINT fact_area_month_hpi_prices_positive
# MAGIC   CHECK (avg_price IS NULL OR avg_price > 0);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_hpi DROP CONSTRAINT IF EXISTS fact_area_month_hpi_volume_not_negative;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_hpi ADD CONSTRAINT fact_area_month_hpi_volume_not_negative
# MAGIC   CHECK (sales_volume IS NULL OR sales_volume >= 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_area_month_rent
# MAGIC
# MAGIC Private rents, roughly 49,000 rows from 2015-01. Scotland and Northern Ireland are published
# MAGIC on broad rental market areas, which conform to nothing below nation, so those rows carry a
# MAGIC `rental_market_area` key and cannot be paired with a price for the same geography.
# MAGIC
# MAGIC Property-type rents are carried because they pair with the house price index property types,
# MAGIC so a yield can be computed per property type.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.fact_area_month_rent (
# MAGIC   area_code                      STRING         NOT NULL,
# MAGIC   month_start_date               DATE           NOT NULL COMMENT 'First of the month. Joins dim_date.date_key.',
# MAGIC   rental_price                   INT                     COMMENT 'Average monthly rent, pounds.',
# MAGIC   price_index                    DECIMAL(18,6)           COMMENT 'Index, 100 at January 2015.',
# MAGIC   pct_change_1m                  DECIMAL(18,6),
# MAGIC   pct_change_12m                 DECIMAL(18,6),
# MAGIC   one_bed_rental_price           INT,
# MAGIC   two_bed_rental_price           INT,
# MAGIC   three_bed_rental_price         INT,
# MAGIC   four_or_more_bed_rental_price  INT,
# MAGIC   detached_rental_price          INT,
# MAGIC   semi_detached_rental_price     INT,
# MAGIC   terraced_rental_price          INT,
# MAGIC   flat_maisonette_rental_price   INT,
# MAGIC   CONSTRAINT fact_area_month_rent_pk PRIMARY KEY (area_code, month_start_date),
# MAGIC   CONSTRAINT fact_area_month_rent_area_fk FOREIGN KEY (area_code) REFERENCES uk_property_intel.gold.dim_area,
# MAGIC   CONSTRAINT fact_area_month_rent_date_fk FOREIGN KEY (month_start_date) REFERENCES uk_property_intel.gold.dim_date
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Monthly private rent series by published area.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_rent DROP CONSTRAINT IF EXISTS fact_area_month_rent_month_start;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_rent ADD CONSTRAINT fact_area_month_rent_month_start
# MAGIC   CHECK (month_start_date = trunc(month_start_date, 'MM'));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_rent DROP CONSTRAINT IF EXISTS fact_area_month_rent_positive;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_rent ADD CONSTRAINT fact_area_month_rent_positive
# MAGIC   CHECK (rental_price IS NULL OR rental_price > 0);
# MAGIC
# MAGIC -- The series begins in January 2015 for every area.
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_rent DROP CONSTRAINT IF EXISTS fact_area_month_rent_floor;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_rent ADD CONSTRAINT fact_area_month_rent_floor
# MAGIC   CHECK (month_start_date >= DATE'2015-01-01');

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_area_month_price
# MAGIC
# MAGIC Transactions aggregated to published area, roughly 120,000 rows from 1995-01. England and
# MAGIC Wales only, since the transaction source covers nothing else.
# MAGIC
# MAGIC Rows exist for districts, their regions, the two nations and the England and Wales composite.
# MAGIC Each level is aggregated from the transactions themselves, because a median cannot be
# MAGIC recovered from the medians below it.
# MAGIC
# MAGIC District is assigned from the postcode, not from the district recorded on the transaction.
# MAGIC The two disagree on 15 percent of rows, falling from 25 percent in 1995 to 5 percent in 2026,
# MAGIC which is local government reorganisation showing up in an old record. Resolving through the
# MAGIC postcode restates history onto current boundaries, matching what both index publishers do.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.fact_area_month_price (
# MAGIC   area_code          STRING   NOT NULL,
# MAGIC   month_start_date   DATE     NOT NULL COMMENT 'First of the month. Joins dim_date.date_key.',
# MAGIC   median_price       INT      NOT NULL COMMENT 'Median transaction price, pounds.',
# MAGIC   mean_price         INT      NOT NULL,
# MAGIC   transaction_count  INT      NOT NULL,
# MAGIC   CONSTRAINT fact_area_month_price_pk PRIMARY KEY (area_code, month_start_date),
# MAGIC   CONSTRAINT fact_area_month_price_area_fk FOREIGN KEY (area_code) REFERENCES uk_property_intel.gold.dim_area,
# MAGIC   CONSTRAINT fact_area_month_price_date_fk FOREIGN KEY (month_start_date) REFERENCES uk_property_intel.gold.dim_date
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Monthly transaction price summary by published area, England and Wales.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_price DROP CONSTRAINT IF EXISTS fact_area_month_price_month_start;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_price ADD CONSTRAINT fact_area_month_price_month_start
# MAGIC   CHECK (month_start_date = trunc(month_start_date, 'MM'));
# MAGIC
# MAGIC -- A row exists because transactions were recorded, so the count is never zero.
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_price DROP CONSTRAINT IF EXISTS fact_area_month_price_measures;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_price ADD CONSTRAINT fact_area_month_price_measures
# MAGIC   CHECK (transaction_count > 0 AND median_price > 0 AND mean_price > 0);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_price DROP CONSTRAINT IF EXISTS fact_area_month_price_floor;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_price ADD CONSTRAINT fact_area_month_price_floor
# MAGIC   CHECK (month_start_date >= DATE'1995-01-01');

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_area_month_transaction_mix
# MAGIC
# MAGIC Transaction counts split by the four attributes the source records about a sale, roughly one
# MAGIC to two million rows. This is the only fact carrying those attributes. They sit in the key, not
# MAGIC in a dimension, because each has a handful of values and no attributes of its own.
# MAGIC
# MAGIC Counts only. They sum cleanly to any coarser cut, and holding the median out keeps it from
# MAGIC appearing at a grain too thin to support one.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.fact_area_month_transaction_mix (
# MAGIC   area_code          STRING  NOT NULL,
# MAGIC   month_start_date   DATE    NOT NULL COMMENT 'First of the month. Joins dim_date.date_key.',
# MAGIC   property_type      STRING  NOT NULL COMMENT 'D detached, S semi-detached, T terraced, F flat, O other.',
# MAGIC   old_new            STRING  NOT NULL COMMENT 'Y newly built, N established.',
# MAGIC   duration           STRING  NOT NULL COMMENT 'F freehold, L leasehold, U unknown.',
# MAGIC   ppd_category_type  STRING  NOT NULL COMMENT 'A full market value, B other including repossessions and buy-to-let portfolios.',
# MAGIC   transaction_count  INT     NOT NULL,
# MAGIC   CONSTRAINT fact_area_month_transaction_mix_pk PRIMARY KEY (area_code, month_start_date, property_type, old_new, duration, ppd_category_type),
# MAGIC   CONSTRAINT fact_area_month_transaction_mix_area_fk FOREIGN KEY (area_code) REFERENCES uk_property_intel.gold.dim_area,
# MAGIC   CONSTRAINT fact_area_month_transaction_mix_date_fk FOREIGN KEY (month_start_date) REFERENCES uk_property_intel.gold.dim_date
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (month_start_date, area_code)
# MAGIC COMMENT 'Monthly transaction counts by property type, build status, tenure and sale category.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_transaction_mix DROP CONSTRAINT IF EXISTS fact_mix_month_start;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_transaction_mix ADD CONSTRAINT fact_mix_month_start
# MAGIC   CHECK (month_start_date = trunc(month_start_date, 'MM'));
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_transaction_mix DROP CONSTRAINT IF EXISTS fact_mix_code_values;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_transaction_mix ADD CONSTRAINT fact_mix_code_values
# MAGIC   CHECK (
# MAGIC     property_type IN ('D', 'S', 'T', 'F', 'O')
# MAGIC     AND old_new IN ('Y', 'N')
# MAGIC     AND duration IN ('F', 'L', 'U')
# MAGIC     AND ppd_category_type IN ('A', 'B')
# MAGIC   );
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_transaction_mix DROP CONSTRAINT IF EXISTS fact_mix_count_positive;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_transaction_mix ADD CONSTRAINT fact_mix_count_positive
# MAGIC   CHECK (transaction_count > 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_area_month_crime and fact_area_month_crime_total
# MAGIC
# MAGIC Crime summed up from small areas, roughly 940,000 and 62,000 rows from 2010-12. England and
# MAGIC Wales only. Northern Ireland publishes no small-area code, so its crime exists at force level
# MAGIC and nowhere below it, and Police Scotland does not publish to this source. Summation stops at
# MAGIC the England and Wales composite for that reason: a United Kingdom total would be a partial
# MAGIC count wearing a whole one's label.
# MAGIC
# MAGIC Anti-social behaviour has no row in `fact_area_month_crime`. Its count sits in the total table
# MAGIC beside a total that excludes it, so a sum over crime types cannot pick it up by accident.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.fact_area_month_crime (
# MAGIC   area_code         STRING  NOT NULL,
# MAGIC   month_start_date  DATE    NOT NULL COMMENT 'First of the month. Joins dim_date.date_key.',
# MAGIC   crime_type        STRING  NOT NULL,
# MAGIC   crime_count       INT     NOT NULL,
# MAGIC   CONSTRAINT fact_area_month_crime_pk PRIMARY KEY (area_code, month_start_date, crime_type),
# MAGIC   CONSTRAINT fact_area_month_crime_area_fk FOREIGN KEY (area_code) REFERENCES uk_property_intel.gold.dim_area,
# MAGIC   CONSTRAINT fact_area_month_crime_date_fk FOREIGN KEY (month_start_date) REFERENCES uk_property_intel.gold.dim_date,
# MAGIC   CONSTRAINT fact_area_month_crime_type_fk FOREIGN KEY (crime_type) REFERENCES uk_property_intel.gold.dim_crime_type
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Monthly crime counts by published area and crime type, anti-social behaviour excluded.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.fact_area_month_crime_total (
# MAGIC   area_code               STRING  NOT NULL,
# MAGIC   month_start_date        DATE    NOT NULL COMMENT 'First of the month. Joins dim_date.date_key.',
# MAGIC   crime_count_excl_asb    INT     NOT NULL COMMENT 'Sum of every type in fact_area_month_crime for this area and month.',
# MAGIC   anti_social_behaviour   INT     NOT NULL COMMENT 'Reported separately. Force-level double counting and a shift from 42 to 16 percent of records make it unfit for comparison or for any total.',
# MAGIC   CONSTRAINT fact_area_month_crime_total_pk PRIMARY KEY (area_code, month_start_date),
# MAGIC   CONSTRAINT fact_area_month_crime_total_area_fk FOREIGN KEY (area_code) REFERENCES uk_property_intel.gold.dim_area,
# MAGIC   CONSTRAINT fact_area_month_crime_total_date_fk FOREIGN KEY (month_start_date) REFERENCES uk_property_intel.gold.dim_date
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Monthly crime totals by published area, with anti-social behaviour held apart.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_crime DROP CONSTRAINT IF EXISTS fact_area_crime_month_start;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_crime ADD CONSTRAINT fact_area_crime_month_start
# MAGIC   CHECK (month_start_date = trunc(month_start_date, 'MM') AND month_start_date >= DATE'2010-12-01');
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_crime DROP CONSTRAINT IF EXISTS fact_area_crime_no_asb;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_crime ADD CONSTRAINT fact_area_crime_no_asb
# MAGIC   CHECK (crime_type <> 'Anti-social behaviour' AND crime_count > 0);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_crime_total DROP CONSTRAINT IF EXISTS fact_area_crime_total_month_start;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_crime_total ADD CONSTRAINT fact_area_crime_total_month_start
# MAGIC   CHECK (month_start_date = trunc(month_start_date, 'MM') AND month_start_date >= DATE'2010-12-01');
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_crime_total DROP CONSTRAINT IF EXISTS fact_area_crime_total_not_negative;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_area_month_crime_total ADD CONSTRAINT fact_area_crime_total_not_negative
# MAGIC   CHECK (crime_count_excl_asb >= 0 AND anti_social_behaviour >= 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_lsoa_month_crime and fact_lsoa_month_crime_total
# MAGIC
# MAGIC The same pair at small-area grain, roughly 25 million and 6.3 million rows. Crime type
# MAGIC accounts for almost all of that: adding it to area and month multiplies the cell count
# MAGIC roughly fivefold.
# MAGIC
# MAGIC Counts are stored as published. Small areas are drawn to hold about 1,500 people, so a count
# MAGIC is loosely population-normalised already, and any rate or rank a screen wants can be derived
# MAGIC from a count while the reverse does not hold.
# MAGIC
# MAGIC A code exclusive to the 2011 boundaries appears here and carries no matching price, and the
# MAGIC 30-month band where both boundary sets are in use means a changed area's series fades out and
# MAGIC in across two and a half years instead of breaking on a date. `dim_lsoa.boundary_vintage`
# MAGIC marks which codes that applies to.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.fact_lsoa_month_crime (
# MAGIC   lsoa_code         STRING  NOT NULL,
# MAGIC   month_start_date  DATE    NOT NULL COMMENT 'First of the month. Joins dim_date.date_key.',
# MAGIC   crime_type        STRING  NOT NULL,
# MAGIC   crime_count       INT     NOT NULL,
# MAGIC   CONSTRAINT fact_lsoa_month_crime_pk PRIMARY KEY (lsoa_code, month_start_date, crime_type),
# MAGIC   CONSTRAINT fact_lsoa_month_crime_lsoa_fk FOREIGN KEY (lsoa_code) REFERENCES uk_property_intel.gold.dim_lsoa,
# MAGIC   CONSTRAINT fact_lsoa_month_crime_date_fk FOREIGN KEY (month_start_date) REFERENCES uk_property_intel.gold.dim_date,
# MAGIC   CONSTRAINT fact_lsoa_month_crime_type_fk FOREIGN KEY (crime_type) REFERENCES uk_property_intel.gold.dim_crime_type
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (month_start_date, lsoa_code)
# MAGIC COMMENT 'Monthly crime counts by small area and crime type, anti-social behaviour excluded.';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.fact_lsoa_month_crime_total (
# MAGIC   lsoa_code              STRING  NOT NULL,
# MAGIC   month_start_date       DATE    NOT NULL COMMENT 'First of the month. Joins dim_date.date_key.',
# MAGIC   crime_count_excl_asb   INT     NOT NULL COMMENT 'Sum of every type in fact_lsoa_month_crime for this area and month.',
# MAGIC   anti_social_behaviour  INT     NOT NULL COMMENT 'Reported separately and excluded from every total.',
# MAGIC   CONSTRAINT fact_lsoa_month_crime_total_pk PRIMARY KEY (lsoa_code, month_start_date),
# MAGIC   CONSTRAINT fact_lsoa_month_crime_total_lsoa_fk FOREIGN KEY (lsoa_code) REFERENCES uk_property_intel.gold.dim_lsoa,
# MAGIC   CONSTRAINT fact_lsoa_month_crime_total_date_fk FOREIGN KEY (month_start_date) REFERENCES uk_property_intel.gold.dim_date
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (month_start_date, lsoa_code)
# MAGIC COMMENT 'Monthly crime totals by small area, with anti-social behaviour held apart.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_month_crime DROP CONSTRAINT IF EXISTS fact_lsoa_crime_month_start;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_month_crime ADD CONSTRAINT fact_lsoa_crime_month_start
# MAGIC   CHECK (month_start_date = trunc(month_start_date, 'MM') AND month_start_date >= DATE'2010-12-01');
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_month_crime DROP CONSTRAINT IF EXISTS fact_lsoa_crime_no_asb;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_month_crime ADD CONSTRAINT fact_lsoa_crime_no_asb
# MAGIC   CHECK (crime_type <> 'Anti-social behaviour' AND crime_count > 0);
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_month_crime_total DROP CONSTRAINT IF EXISTS fact_lsoa_crime_total_month_start;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_month_crime_total ADD CONSTRAINT fact_lsoa_crime_total_month_start
# MAGIC   CHECK (month_start_date = trunc(month_start_date, 'MM') AND month_start_date >= DATE'2010-12-01');
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_month_crime_total DROP CONSTRAINT IF EXISTS fact_lsoa_crime_total_not_negative;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_month_crime_total ADD CONSTRAINT fact_lsoa_crime_total_not_negative
# MAGIC   CHECK (crime_count_excl_asb >= 0 AND anti_social_behaviour >= 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## fact_lsoa_year_price
# MAGIC
# MAGIC Transaction prices at small-area grain, 1.14 million rows from 1995.
# MAGIC
# MAGIC Annual because monthly is too thin to carry a median. At small area and month, 3.2 million
# MAGIC cells hold one transaction and 2.7 million hold two, so the median would describe an
# MAGIC individual property. At small area and year, 998,000 of 1,135,000 cells hold eleven or more.
# MAGIC
# MAGIC Transactions are attributed to 2021 boundary codes only. No crosswalk is applied, so an area
# MAGIC whose code changed carries its price under the new code and its earlier crime under the old
# MAGIC one.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS uk_property_intel.gold.fact_lsoa_year_price (
# MAGIC   lsoa_code          STRING  NOT NULL,
# MAGIC   year_start_date    DATE    NOT NULL COMMENT 'First of January. Joins dim_date.date_key.',
# MAGIC   median_price       INT     NOT NULL COMMENT 'Median transaction price, pounds.',
# MAGIC   mean_price         INT     NOT NULL,
# MAGIC   transaction_count  INT     NOT NULL,
# MAGIC   CONSTRAINT fact_lsoa_year_price_pk PRIMARY KEY (lsoa_code, year_start_date),
# MAGIC   CONSTRAINT fact_lsoa_year_price_lsoa_fk FOREIGN KEY (lsoa_code) REFERENCES uk_property_intel.gold.dim_lsoa,
# MAGIC   CONSTRAINT fact_lsoa_year_price_date_fk FOREIGN KEY (year_start_date) REFERENCES uk_property_intel.gold.dim_date
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (year_start_date, lsoa_code)
# MAGIC COMMENT 'Annual transaction price summary by small area, England and Wales.';

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_year_price DROP CONSTRAINT IF EXISTS fact_lsoa_price_year_start;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_year_price ADD CONSTRAINT fact_lsoa_price_year_start
# MAGIC   CHECK (year_start_date = trunc(year_start_date, 'YEAR') AND year_start_date >= DATE'1995-01-01');
# MAGIC
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_year_price DROP CONSTRAINT IF EXISTS fact_lsoa_price_measures;
# MAGIC ALTER TABLE uk_property_intel.gold.fact_lsoa_year_price ADD CONSTRAINT fact_lsoa_price_measures
# MAGIC   CHECK (transaction_count > 0 AND median_price > 0 AND mean_price > 0);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN uk_property_intel.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED uk_property_intel.gold.fact_lsoa_month_crime;
