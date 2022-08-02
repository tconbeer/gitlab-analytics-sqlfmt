with
    source as (select * from {{ source("greenhouse", "stages") }}),
    renamed as (

        select

            -- keys
            id::number as stage_id,
            organization_id::number as organization_id,

            -- info
            name::varchar as stage_name,
            "order"::number as stage_order,
            active::boolean as is_active


        from source

    ),
    final as (

        select
            *,
            case
                when lower(stage_name) like '%screen%'
                then 'Screen'
                when lower(stage_name) like '%executive interview%'
                then 'Executive Interview'
                when lower(stage_name) like '%interview%'
                then 'Team Interview - Face to Face'
                when lower(stage_name) like '%assessment%'
                then 'Take Home Assessment'
                when lower(stage_name) like '%take home%'
                then 'Take Home Assessment'
                when stage_name in ('Hiring Manager Review', 'Preliminary Phone Screen')
                then 'Hiring Manager Review'
                when lower(stage_name) like '%reference%'
                then 'Reference Check'
                when lower(stage_name) like '%background check & offer'
                then 'Offer'
                else stage_name
            end::varchar(100) as stage_name_modified,
            iff(
                stage_name_modified in (
                    'Application Review',
                    'Screen',
                    'Hiring Manager Review',
                    'Take Home Assessment',
                    'Executive Interview',
                    'Reference Check',
                    'Offer'
                ),
                true,
                false
            ) as is_milestone_stage
        from renamed

    )

select *
from final
