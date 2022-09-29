with
    source as (

        select *
        from {{ ref("gitlab_dotcom_ci_runners_dedupe_source") }}
        where created_at is not null

    ),
    renamed as (

        select
            id::number as runner_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            description::varchar as description,
            contacted_at::timestamp as contacted_at,
            active::boolean as is_active,
            name::varchar as runner_name,
            version::varchar as version,
            revision::varchar as revision,
            platform::varchar as platform,
            architecture::varchar as architecture,
            run_untagged::boolean as is_untagged,
            locked::boolean as is_locked,
            access_level::number as access_level,
            ip_address::varchar as ip_address,
            maximum_timeout::number as maximum_timeout,
            runner_type::number as runner_type,
            public_projects_minutes_cost_factor::float
            as public_projects_minutes_cost_factor,
            private_projects_minutes_cost_factor::float
            as private_projects_minutes_cost_factor
        from source

    )

select *
from renamed
