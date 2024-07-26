{{
    config(
        {
            "schema": "sensitive",
            "database": env_var("SNOWFLAKE_PREP_DATABASE"),
        }
    )
}}

with
    source as (select * from {{ source("zendesk", "ticket_comments") }}),

    renamed as (

        select

            -- ids
            audit_id,
            author_id,
            id as ticket_comment_id,
            ticket_id,

            -- field
            body as comment_body,
            html_body as comment_html_body,
            plain_body as comment_plain_body,
            public as is_public,
            "TYPE" as comment_type,

            -- dates
            created_at

        from source

    )

select *
from renamed
