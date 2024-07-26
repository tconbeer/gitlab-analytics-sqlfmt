with
    source as (

        select {{ hash_sensitive_columns("bizible_site_links_source") }}
        from {{ ref("bizible_site_links_source") }}

    )

select *
from source
