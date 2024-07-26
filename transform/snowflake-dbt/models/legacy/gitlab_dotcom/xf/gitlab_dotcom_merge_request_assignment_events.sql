with
    users as (

        select user_id, user_name
        from {{ ref("gitlab_dotcom_users") }} users
        where {{ filter_out_blocked_users("users", "user_id") }}

    ),
    notes as (

        select
            *,
            iff(note like 'Reassigned%', note, null) as reassigned,
            iff(
                note like 'assigned%', split_part(note, 'unassigned ', 1), null
            ) as assigned,
            iff(
                note like '%unassigned%', split_part(note, 'unassigned ', 2), null
            ) as unassigned
        from {{ ref("gitlab_dotcom_internal_notes_xf") }}
        where
            noteable_type = 'MergeRequest'
            and (
                note like 'assigned to%'
                or note like 'unassigned%'
                or note like 'Reassigned%'
            )

    ),
    notes_cleaned as (

        select
            note_id,
            noteable_id,
            note_author_id,
            created_at,
            note,
            event,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                event_string, '(?<=\@)(.*?)(?=(\\s|$|\,))'
            ) as event_cleaned
        from
            notes unpivot (event_string for event in (assigned, unassigned, reassigned))

    ),
    notes_flat as (

        select notes_cleaned.*, f.index as rank_in_event, f.value as user_name
        from notes_cleaned, lateral flatten(input => event_cleaned) f

    ),
    joined as (

        select
            noteable_id as merge_request_id,
            note_id,
            note_author_id,
            created_at as note_created_at,
            lower(event) as event,
            user_id as event_user_id,
            rank_in_event
        from notes_flat
        inner join users on notes_flat.user_name = users.user_name
        where {{ filter_out_blocked_users("notes_flat", "note_author_id") }}

    )

select *
from joined
order by 1, 2, 7
