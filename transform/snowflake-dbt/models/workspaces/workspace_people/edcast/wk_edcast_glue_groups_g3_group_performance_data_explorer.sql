
with
    source as (

        select
            {{
                hash_sensitive_columns(
                    "edcast_glue_groups_g3_group_performance_data_explorer"
                )
            }}
        from {{ ref("edcast_glue_groups_g3_group_performance_data_explorer") }}

    )

select *
from source
