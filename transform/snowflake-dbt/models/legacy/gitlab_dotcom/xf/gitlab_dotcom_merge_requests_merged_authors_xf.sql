{{ config(tags=["mnpi_exception"]) }}

with
    merge_requests as (select * from {{ ref("gitlab_dotcom_merge_requests_xf") }}),
    notes as (

        select noteable_id, note_author_id, note
        from {{ ref("gitlab_dotcom_notes_xf") }}

    ),
    users as (select user_id, user_name from {{ ref("gitlab_dotcom_users_xf") }}),
    joined_to_mr as (

        select
            merge_requests.project_id,
            merge_requests.namespace_id,
            merge_requests.merge_request_iid,
            merge_requests.merge_request_title,
            merge_requests.merge_request_id,
            notes.note_author_id,
            users.user_name
        from merge_requests
        inner join notes on merge_requests.merge_request_id = notes.noteable_id
        inner join users on notes.note_author_id = users.user_id
        where notes.note = 'merged'

    )

select *
from joined_to_mr
