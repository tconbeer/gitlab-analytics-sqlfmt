
with
    source as (select * from {{ ref("gitlab_dotcom_epics_dedupe_source") }}),
    renamed as (

        select
            id::number as epic_id,
            group_id::number as group_id,
            author_id::number as author_id,
            assignee_id::number as assignee_id,
            iid::number as epic_internal_id,
            updated_by_id::number as updated_by_id,
            last_edited_by_id::number as last_edited_by_id,
            lock_version::number as lock_version,
            start_date::date as epic_start_date,
            end_date::date as epic_end_date,
            last_edited_at::timestamp as epic_last_edited_at,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at,
            title::varchar as epic_title,
            description::varchar as epic_description,
            closed_at::timestamp as closed_at,
            state_id::number as state_id,
            parent_id::number as parent_id,
            relative_position::number as relative_position,
            start_date_sourcing_epic_id::number as start_date_sourcing_epic_id,
            external_key::varchar as external_key,
            confidential::boolean as is_confidential,
            {{ map_state_id("state_id") }} as state,
            length(title)::number as epic_title_length,
            length(description)::number as epic_description_length

        from source

    )

select *
from renamed
