with
    source as (

        select * from {{ ref("gitlab_dotcom_deployment_merge_requests_dedupe_source") }}

    ),
    renamed as (

        select
            deployment_id::number as deployment_id,
            merge_request_id::number as merge_request_id,
            md5(deployment_merge_request_id::varchar) as deployment_merge_request_id
        from source

    )

select *
from renamed
