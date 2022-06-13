
{{
    simple_cte(
        [
            ("saas_usage_ping_namespace", "saas_usage_ping_namespace"),
            ("dim_date", "dim_date"),
        ]
    )
}}

,
transformed as (

    select
        ping_name,
        ping_date,
        ping_level,
        iff(error = 'Success', true, false) as is_success,
        count(distinct namespace_ultimate_parent_id) as namespace_with_value
    from saas_usage_ping_namespace
    group by 1, 2, 3, 4

),
joined as (

    select transformed.* from transformed inner join dim_date on ping_date = date_day

)

select *
from joined
