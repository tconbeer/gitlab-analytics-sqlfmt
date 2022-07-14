{{ config(tags=["product"]) }}

with
    source as (

        select *
        from {{ ref("usage_ping_metrics_source") }}
        qualify max(uploaded_at) OVER () = uploaded_at

    )

select *
from source
