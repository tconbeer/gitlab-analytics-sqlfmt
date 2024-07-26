{% set fields_to_mask = ["note"] %}

with
    prep_note as (

        select
            -- PRIMARY KEY
            dim_note_id,

            -- FOREIGN KEY
            author_id,
            dim_project_id,
            ultimate_parent_namespace_id,
            noteable_id,
            created_date_id,
            dim_plan_id,

            -- METADATA
            noteable_type,
            created_at,
            updated_at,
            -- note, sensitive info so masked
            attachment,
            line_code,
            commit_id,
            is_system_note,
            note_updated_by_id,
            position_number,
            original_position,
            resolved_at,
            resolved_by_id,
            discussion_id,
            cached_markdown_version,
            resolved_by_push
        from {{ ref("prep_note") }}

    )

    {{
        dbt_audit(
            cte_ref="prep_note",
            created_by="@mpeychet_",
            updated_by="@mpeychet_",
            created_date="2021-06-22",
            updated_date="2021-06-22",
        )
    }}
