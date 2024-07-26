with
    source as (

        select {{ hash_sensitive_columns("bizible_page_views_source") }}
        from {{ ref("bizible_page_views_source") }}

    )

select *
from source
