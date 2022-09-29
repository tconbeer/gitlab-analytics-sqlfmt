
with
    source as (select * from {{ ref("gitlab_dotcom_deployments_dedupe_source") }}),
    renamed as (

        select
            id::number as deployment_id,
            iid::number as deployment_iid,
            project_id::number as project_id,
            environment_id::number as environment_id,
            ref::varchar as ref,
            tag::boolean as tag,
            sha::varchar as sha,
            user_id::number as user_id,
            deployable_id::number as deployable_id,
            deployable_type::varchar as deployable_type,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            on_stop::varchar as on_stop,
            finished_at::timestamp as finished_at,
            status::number as status_id,
            cluster_id::number as cluster_id
        from source

    )

select *
from renamed
