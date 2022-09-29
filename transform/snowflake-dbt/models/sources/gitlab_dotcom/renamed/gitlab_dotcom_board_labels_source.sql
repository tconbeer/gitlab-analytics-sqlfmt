with
    source as (select * from {{ ref("gitlab_dotcom_board_labels_dedupe_source") }}),
    renamed as (

        select
            id::number as board_label_relation_id,
            board_id::number as board_id,
            label_id::number as label_id

        from source

    )


select *
from renamed
