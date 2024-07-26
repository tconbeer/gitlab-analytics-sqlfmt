with
    session_custom_dims as (select * from {{ ref("ga360_session_custom_dimension") }}),
    ga_index_names as (

        select * from {{ ref("google_analytics_custom_dimension_indexes") }}

    ),
    named_dims as (

        select
            -- dimensions
            session_custom_dims.*,

            -- index names
            ga_index_names.name as dimension_name

        from session_custom_dims
        left join
            ga_index_names on session_custom_dims.dimension_index = ga_index_names.index

    )

select *
from named_dims
