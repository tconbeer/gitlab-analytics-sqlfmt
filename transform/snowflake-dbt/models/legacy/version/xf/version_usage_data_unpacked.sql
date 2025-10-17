{% set version_usage_stats_list = dbt_utils.get_column_values(
    table=ref("version_usage_stats_list"),
    column="full_ping_name",
    max_records=1000,
    default=[""],
) %}

{{ config({"materialized": "incremental", "unique_key": "id"}) }}

with
    usage_data_unpacked_intermediate as (

        select *
        from {{ ref("version_usage_data_unpacked_intermediate") }}
        {% if is_incremental() %}

            where created_at >= (select max(created_at) from {{ this }})

        {% endif %}

    ),
    transformed as (

        select
            id,
            {{
                dbt_utils.star(
                    from=ref("version_usage_data_unpacked_intermediate"),
                    except=(version_usage_stats_list | upper),
                )
            }},
            gitpod_enabled,
            {% for stat_name in version_usage_stats_list %}

                iff({{ stat_name }} = -1, null, {{ stat_name }}) as {{ stat_name }}
                {{ "," if not loop.last }}
            {% endfor %}
        from usage_data_unpacked_intermediate

    )

select *
from transformed
