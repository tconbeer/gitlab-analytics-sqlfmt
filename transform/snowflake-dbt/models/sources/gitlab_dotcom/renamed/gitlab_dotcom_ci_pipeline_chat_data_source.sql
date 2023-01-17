with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_pipeline_chat_data_dedupe_source") }}

    ),
    renamed as (

        select
            pipeline_id::number as ci_pipeline_id,
            chat_name_id::number as chat_name_id,
            response_url as response_url

        from source

    )

select *
from renamed
