{% macro gdpr_delete_gitlab_dotcom(email_sha, run_queries=False) %}
{% set data_types = (
    "BOOLEAN",
    "TIMESTAMP_TZ",
    "TIMESTAMP_NTZ",
    "FLOAT",
    "DATE",
    "NUMBER",
) %}
{% set exclude_columns = (
    "dbt_scd_id",
    "dbt_updated_at",
    "dbt_valid_from",
    "dbt_valid_to",
    "_task_instance",
    "_uploaded_at",
    "_sdc_batched_at",
    "_sdc_extracted_at",
    "_sdc_received_at",
    "_sdc_sequence",
    "_sdc_table_version",
) %}

        {% set set_sql %}
        SET email_sha = '{{email_sha}}';
        {% endset %}
{{ log(set_sql, info=True) }}

{# DELETE FROM EVERYTHING THAT'S NOT SNAPSHOTS#}
{%- call statement("gdpr_deletions", fetch_result=True) %}

with
    email_columns as (

        select
            lower(table_catalog)
            || '.'
            || lower(table_schema)
            || '.'
            || lower(table_name) as fqd_name,
            listagg(column_name, ',') as email_column_names
        from "RAW"."INFORMATION_SCHEMA"."COLUMNS"
        where
            lower(column_name) like '%email%'
            and table_schema in ('TAP_POSTGRES')
            and data_type not in {{ data_types }}
        group by 1

    )

select fqd_name, email_column_names
from email_columns
;

{%- endcall -%}

{%- set value_list = load_result("gdpr_deletions") -%}

{%- if value_list and value_list["data"] -%}

{%- set values = value_list["data"] %}

{% for data_row in values %}

{% set fqd_name = data_row[0] %}
{% set email_column_list = data_row[1].split(",") %}

{% for email_column in email_column_list %}

            {% set delete_sql %}
                DELETE FROM {{fqd_name}} WHERE SHA2(TRIM(LOWER("{{email_column}}"))) =  '{{email_sha}}';
            {% endset %}
{{ log(delete_sql, info=True) }}

{% if run_queries %}
{% set results = run_query(delete_sql) %} {% set rows_deleted = results.print_table() %}
{% endif %}

{% endfor %}

{% endfor %}

{%- endif -%}


{# UPDATE SNAPSHOTS #}
{%- call statement("update_snapshots", fetch_result=True) %}

with
    email_columns as (

        select
            lower(table_catalog)
            || '.'
            || lower(table_schema)
            || '.'
            || lower(table_name) as fqd_name,
            listagg(column_name, ',') as email_column_names
        from "RAW"."INFORMATION_SCHEMA"."COLUMNS"
        where
            lower(column_name) like '%email%'
            and table_schema in ('SNAPSHOTS')
            and data_type not in {{ data_types }}
            and lower(column_name) not in {{ exclude_columns }}
            and lower(table_name) like ('gitlab_dotcom_%')
        group by 1

    ),
    non_email_columns as (

        select
            lower(table_catalog)
            || '.'
            || lower(table_schema)
            || '.'
            || lower(table_name) as fqd_name,
            listagg(column_name, ',') as non_email_column_names
        from "RAW"."INFORMATION_SCHEMA"."COLUMNS" as a
        where
            lower(column_name) not like '%email%'
            and table_schema in ('SNAPSHOTS')
            and data_type not in {{ data_types }}
            and lower(column_name) not in {{ exclude_columns }}
            and lower(column_name) not like '%id%'
            and lower(column_name) not in {{ exclude_columns }}
            and lower(table_name) like ('gitlab_dotcom_%')
        group by 1

    )

select a.fqd_name, a.email_column_names, b.non_email_column_names
from email_columns a
left join non_email_columns b on a.fqd_name = b.fqd_name
;

{%- endcall -%}

{%- set value_list = load_result("update_snapshots") -%}

{%- if value_list and value_list["data"] -%}

{%- set values = value_list["data"] %}

{% for data_row in values %}

{% set fqd_name = data_row[0] %}
{% set email_column_list = data_row[1].split(",") %}
{% set non_email_column_list = data_row[2].split(",") %}

{% for email_column in email_column_list %}

            {% set sql %}
                UPDATE {{fqd_name}} SET
                {% for non_email_column in non_email_column_list -%}
                    {{non_email_column}} =  'GDPR Redacted'{% if not loop.last %}, {% endif %}
                {% endfor %}
                WHERE SHA2(TRIM(LOWER("{{email_column}}"))) =  '{{email_sha}}';

            {% endset %}
{{ log(sql, info=True) }}

{% if run_queries %}
{% set results = run_query(sql) %} {% set rows_updated = results.print_table() %}
{% endif %}

{% endfor %}

{% for email_column in email_column_list %}

            {% set email_sql %}
                UPDATE {{fqd_name}} SET
                {% for email_column_inner in email_column_list -%}
                    {{email_column_inner}} =  '{{email_sha}}'{% if not loop.last %}, {% endif %}
                {% endfor %}
                WHERE SHA2(TRIM(LOWER("{{email_column}}"))) =  '{{email_sha}}';

            {% endset %}
{{ log(email_sql, info=True) }}

{% if email_sql %}
{% set results = run_query(email_sql) %} {% set rows_updated = results.print_table() %}
{% endif %}

{% endfor %}

{% endfor %}

{%- endif -%}

{{ log("Removal Complete!", info=True) }}

{%- endmacro -%}
