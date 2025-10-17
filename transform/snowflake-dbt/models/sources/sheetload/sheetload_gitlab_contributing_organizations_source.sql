with
    source as (

        select * from {{ source("sheetload", "gitlab_contributing_organizations") }}

    ),
    renamed as (

        select
            contributor_organization::varchar as contributor_organization,
            contributor_usernames::varchar as contributor_usernames,
            sfdc_account_id::varchar as sfdc_account_id
        from source

    )

select *
from renamed
