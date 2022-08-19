{% set version_usage_stats_list = dbt_utils.get_column_values(
    table=ref("version_usage_stats_list"),
    column="full_ping_name",
    default=[""],
) %}

{{ config({"materialized": "incremental", "unique_key": "id"}) }}

with
    usage_data as (

        select *
        from {{ ref("version_usage_data_with_metadata") }}
        {% if is_incremental() %}

        where created_at >= (select max(created_at) from {{ this }})

        {% endif %}

    ),
    stats_used_unpacked as (

        select id, full_ping_name, ping_value
        from {{ ref("version_usage_data_unpacked_stats_used") }}
        {% if is_incremental() %}

        where created_at >= (select max(created_at) from {{ this }})

        {% endif %}

    ),
    pivoted as (

        select *
        from
            stats_used_unpacked
            pivot(
                max(ping_value)
                for full_ping_name
                in ({{ "'" + version_usage_stats_list | join("',\n '") + "'" }})
            ) as pivoted_table(id, {{ "\n" + version_usage_stats_list | join(",\n") }})

    ),
    final as (

        select
            {{
                dbt_utils.star(
                    from=ref("version_usage_data_with_metadata"),
                    except=[
                        "ID",
                        "STATS_USED",
                        "COUNTS",
                        "USAGE_ACTIVITY_BY_STAGE",
                        "ANALYTICS_UNIQUE_VISIT",
                    ],
                    relation_alias="usage_data",
                )
            }}, pivoted.*

        from usage_data
        left join pivoted on usage_data.id = pivoted.id

    )

select *
from final
