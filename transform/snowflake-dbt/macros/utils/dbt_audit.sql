{%- macro dbt_audit(cte_ref, created_by, updated_by, created_date, updated_date) -%}

select
    *,
    '{{ created_by }}'::varchar as created_by,
    '{{ updated_by }}'::varchar as updated_by,
    '{{ created_date }}'::date as model_created_date,
    '{{ updated_date }}'::date as model_updated_date,
    current_timestamp() as dbt_updated_at,

    {% if execute %}

    {% if not flags.FULL_REFRESH and config.get("materialized") == "incremental" %}

    {%- set source_relation = adapter.get_relation(
    database=target.database,
    schema=this.schema,
    identifier=this.table,
) -%}

    {% if source_relation != None %}

    {% set min_created_date %}
                    SELECT LEAST(MIN(dbt_created_at), CURRENT_TIMESTAMP()) AS min_ts 
                    FROM {{ this }}
    {% endset %}

    {% set results = run_query(min_created_date) %}

    '{{results.columns[0].values()[0]}}'::timestamp as dbt_created_at

    {% else %} current_timestamp() as dbt_created_at

    {% endif %}

    {% else %} current_timestamp() as dbt_created_at

    {% endif %}

    {% endif %}

from {{ cte_ref }}

{%- endmacro -%}
