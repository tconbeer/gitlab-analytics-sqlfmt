with
    source as (

        select * from {{ ref("gitlab_dotcom_ci_build_trace_chunks_dedupe_source") }}

    ),
    renamed as (

        select
            build_id::number as ci_build_id,
            chunk_index::varchar as chunk_index,
            data_store::varchar as data_store,
            raw_data::varchar as raw_data

        from source

    )


select *
from renamed
