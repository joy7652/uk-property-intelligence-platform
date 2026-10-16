{#
  Schemas are declared in databricks_src/setup/01_create_schemas.py. dbt creates any
  schema it cannot find before on-run-start hooks run, with a bare CREATE SCHEMA IF NOT
  EXISTS, and the setup statement would then skip everything it declares.
#}
{% macro databricks__create_schema(relation) -%}
  {{ exceptions.raise_compiler_error(
      "Schema " ~ relation.without_identifier() ~ " does not exist. It is created by "
      ~ "databricks_src/setup/01_create_schemas.py, never by dbt."
  ) }}
{%- endmacro %}
