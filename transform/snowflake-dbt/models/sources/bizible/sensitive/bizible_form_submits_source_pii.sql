with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "bizible_form_submits_source", "form_submit_id"
                )
            }}
        from {{ ref("bizible_form_submits_source") }}

    )

select *
from source
