with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "edcast_sheetload_gitlab_certification_tracking_dashboard", "user"
                )
            }}
        from {{ ref("edcast_sheetload_gitlab_certification_tracking_dashboard") }}

    )

select *
from source
