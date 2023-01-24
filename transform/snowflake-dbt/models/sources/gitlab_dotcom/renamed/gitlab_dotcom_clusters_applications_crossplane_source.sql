with
    source as (

        select *
        from {{ ref("gitlab_dotcom_clusters_applications_crossplane_dedupe_source") }}

    ),

    renamed as (

        select
            id::number as clusters_applications_crossplane_id,
            cluster_id::number as cluster_id,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            status::number as status,
            version::varchar as version,
            status_reason::varchar as status_reason,
            stack::varchar as stack
        from source

    )

select *
from renamed
