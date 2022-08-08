with
    source as (

        select
            {{
                nohash_sensitive_columns(
                    "bizible_email_to_visitor_ids_source", "email_to_visitor_id"
                )
            }}
        from {{ ref("bizible_email_to_visitor_ids_source") }}

    )

select *
from source
