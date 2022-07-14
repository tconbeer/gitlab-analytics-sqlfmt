{{ config(materialized="ephemeral") }}

with
    source as (

        select *
        from {{ ref("dim_issue") }}
        where
            ultimate_parent_namespace_id in (6543, 9970)
            and array_contains('infradev'::variant, labels)
    )

select *
from source
