
{% macro mysql__list_schemas(database) -%}
  {% set sql %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.SCHEMATA
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}


{% macro mysql__information_schema_name(database) -%}
  INFORMATION_SCHEMA
{% endmacro %}


{% macro mysql__create_schema(database_name, schema_name) -%}
  {%- call statement('create_schema') -%}
    create database if not exists {{ database_name }}
  {% endcall %}
{% endmacro %}


{% macro mysql__drop_schema(database_name, schema_name) -%}
  {%- call statement('drop_schema') -%}
    drop database if exists {{ database_name }}
  {% endcall %}
{% endmacro %}


{% macro mysql__list_relations_without_caching(information_schema, schema) %}
  {% call statement('list_relations_without_caching', fetch_result=True) -%}
    select
      '{{ information_schema.database }}' as "database",
      table_name as name,
      null as "schema",
      case
        when table_type = 'BASE TABLE' then 'table'
        when table_type = 'VIEW' then 'view'
      end as type
    from information_schema.tables
    where lower(table_schema) like '{{ information_schema.database }}'
  {% endcall %}
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro mysql__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    drop {{ relation.type }} if exists {{ relation }}
  {%- endcall %}
{% endmacro %}


{% macro mysql__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}

  create {% if temporary: -%}temporary{%- endif %} table
    {{ relation.include(database=(not temporary)) }}
  as (
    {{ sql }}
  );
{% endmacro %}


{% macro mysql__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    rename table {{ from_relation }} to {{ target_name }}
  {%- endcall %}
{% endmacro %}


{% macro mysql__alter_column_type(relation, column_name, new_column_type) -%}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} add column {{ adapter.quote(tmp_column) }} {{ new_column_type }};
    update {{ relation }} set {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
    alter table {{ relation }} drop column {{ adapter.quote(column_name) }} cascade;
    alter table {{ relation }} rename column {{ adapter.quote(tmp_column) }} to {{ adapter.quote(column_name) }}
  {% endcall %}

{% endmacro %}


{% macro mysql__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
        select count(*)
        from {{ information_schema.replace(information_schema_view='SCHEMATA') }}
        where schema_name='{{ schema }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}


{% macro mysql__truncate_relation(relation) -%}
  {% call statement('truncate_relation') -%}
    truncate table {{ relation }}
  {%- endcall %}
{% endmacro %}


{% macro mysql__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
      select
          column_name,
          data_type,
          character_maximum_length,
          numeric_precision,
          numeric_scale
      from information_schema.columns
      where table_name = '{{ relation.identifier }}'
        {% if relation.schema %}
        and table_schema = '{{ relation.schema }}'
        {% endif %}
      order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro mysql__make_temp_relation(base_relation, suffix) %}
    {% set tmp_identifier = "temp_" ~ base_relation.identifier %}
    {% set tmp_relation = base_relation.incorporate(
                                path={"identifier": tmp_identifier}) -%}
	
	create temp
	
    {% do return(tmp_relation) %}
{% endmacro %}

