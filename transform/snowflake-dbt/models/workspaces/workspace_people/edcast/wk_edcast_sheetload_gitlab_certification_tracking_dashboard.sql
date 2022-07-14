
with
    source as (

        select
            {{
                hash_sensitive_columns(
                    "edcast_sheetload_gitlab_certification_tracking_dashboard"
                )
            }}
        from {{ ref("edcast_sheetload_gitlab_certification_tracking_dashboard") }}

    )

select *
from source
