with
    source as (

        select
            {{ nohash_sensitive_columns("bizible_page_views_source", "page_view_id") }}
        from {{ ref("bizible_page_views_source") }}

    )

select *
from source
