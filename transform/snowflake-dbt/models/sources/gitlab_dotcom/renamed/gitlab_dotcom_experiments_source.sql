with
    source as (select * from {{ ref("gitlab_dotcom_experiments_dedupe_source") }}),
    renamed as (

        select id::number as experiment_id, name::varchar as experiment_name from source

    )

select *
from renamed
