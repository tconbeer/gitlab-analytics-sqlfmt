
with
    source as (

        select *
        from {{ source("edcast", "glue_groups_g3_group_performance_data_explorer") }}

    ),
    deduplicated as (

        select *
        from source
        qualify
            row_number() over (
                partition by
                    ecl_id,
                    time,
                    group_name,
                    card_resource_url,
                    assigned_content,
                    event,
                    card_author_full_name,
                    follower_user_full_name_,
                    following_user_full_name_,
                    user_full_name,
                    __loaded_at
                order by __loaded_at desc
            ) = 1

    ),
    renamed as (

        select
            assigned_content::boolean as assigned_content,
            nullif(card_author_full_name, '')::varchar as card_author_full_name,
            nullif(card_resource_url, '')::varchar as card_resource_url,
            nullif(card_state, '')::varchar as card_state,
            nullif(card_subtype, '')::varchar as card_subtype,
            nullif(card_title, '')::varchar as card_title,
            nullif(card_type, '')::varchar as card_type,
            nullif(comment_message, '')::varchar as comment_message,
            nullif(comment_status, '')::varchar as comment_status,
            nullif(content_status, '')::varchar as content_status,
            nullif(content_structure, '')::varchar as content_structure,
            nullif(country, '')::varchar as country,
            nullif(department, '')::varchar as department,
            nullif(division, '')::varchar as division,
            nullif(duration_hh_mm_, '')::varchar as duration_hh_mm,
            nullif(ecl_id, '')::varchar as ecl_id,
            nullif(ecl_source_name, '')::varchar as ecl_source_name,
            nullif(email, '')::varchar as email,
            nullif(event, '')::varchar as event,
            excluded_from_leaderboard::boolean as excluded_from_leaderboard,
            nullif(follower_user_full_name_, '')::varchar as follower_user_full_name,
            nullif(following_user_full_name_, '')::varchar as following_user_full_name,
            nullif(gitlab_internal, '')::boolean as gitlab_internal,
            nullif(group_name, '')::varchar as group_name,
            nullif(group_status, '')::varchar as group_status,
            nullif(hire_date, '')::date as hire_date,
            nullif(impartner_account, '')::varchar as impartner_account,
            nullif(is_card_promoted, '')::boolean as is_card_promoted,
            nullif(is_live_stream, '')::boolean as is_live_stream,
            nullif(is_manager, '')::boolean as is_manager,
            nullif(is_public_, '')::boolean as is_public,
            nullif(job_groups, '')::varchar as job_groups,
            nullif(performance_metric, '')::varchar as performance_metric,
            nullif(platform, '')::varchar as platform,
            nullif(region, '')::varchar as region,
            nullif(role, '')::varchar as role_name,
            nullif(shared_to_group_name, '')::varchar as shared_to_group_name,
            nullif(shared_to_user_full_name, '')::varchar as shared_to_user_full_name,
            sign_in_count::number as sign_in_count,
            nullif(standard_card_type, '')::varchar as standard_card_type,
            nullif(supervisor, '')::varchar as supervisor,
            nullif(supervisor_email, '')::varchar as supervisor_email,
            time::timestamp as time,
            time_account_created::timestamp as time_account_created,
            nullif(title, '')::varchar as title,
            nullif(user_account_status, '')::varchar as user_account_status,
            nullif(user_full_name, '')::varchar as user_full_name,
            __loaded_at::timestamp as __loaded_at
        from deduplicated

    )

select *
from renamed
