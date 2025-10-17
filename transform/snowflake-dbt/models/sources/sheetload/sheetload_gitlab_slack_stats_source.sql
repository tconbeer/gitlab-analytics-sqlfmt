with
    source as (select * from {{ source("sheetload", "gitlab_slack_stats") }}),
    renamed as (

        select
            date::date as entry_date,
            -- running totals
            total_full_members::number as full_members,
            total_guests::number as guests,
            public_channels_in_single_workspace::number
            as public_channels_in_single_workspace,
            total_enabled_membership::number as total_membership,
            -- daily totals
            daily_active_members::number as daily_active_members,
            daily_members_posting_messages::number as daily_members_posting_messages,
            files_uploaded::number as files_uploaded,
            messages_in_dms::number as messages_in_dms,
            messages_in_private_channels::number as messages_in_private_channels,
            messages_in_public_channels::number as messages_in_public_channels,
            messages_in_shared_channels::number as messages_in_shared_channels,
            messages_posted::number as messages_posted,
            messages_posted_by_apps::number as messages_posted_by_apps,
            messages_posted_by_members::number as messages_posted_by_members,
            percent_of_messages_in_dms::float as percent_of_messages_in_dms,
            percent_of_messages_in_private_channels::float
            as percent_of_messages_in_private_channels,
            percent_of_messages_in_public_channels::float
            as percent_of_messages_in_public_channels,
            percent_of_views_in_dms::float as percent_of_views_in_dms,
            percent_of_views_in_private_channels::float
            as percent_of_views_in_private_channels,
            percent_of_views_in_public_channels::float
            as percent_of_views_in_public_channels,
            weekly_active_members::number as weekly_active_members,
            weekly_members_posting_messages::number as weekly_members_posting_messages
        from source

    )

select *
from renamed
