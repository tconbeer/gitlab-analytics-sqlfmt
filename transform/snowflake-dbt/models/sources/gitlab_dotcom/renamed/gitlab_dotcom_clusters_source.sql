with
    source as (select * from {{ ref("gitlab_dotcom_clusters_dedupe_source") }}),
    renamed as (

        select
            id::number as cluster_id,
            user_id::number as user_id,
            provider_type::number as provider_type_id,
            platform_type::number as platform_type_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            enabled::boolean as is_enabled,
            environment_scope::varchar as environment_scope,
            cluster_type::number as cluster_type_id,
            domain::varchar as domain,
            managed::varchar as managed

        from source

    )

select *
from renamed
