with
    source as (

        select *
        from {{ source("engineering", "development_team_members") }}
        order by uploaded_at desc
        limit 1

    ),
    flattened as (

        select d.value as data_by_row
        from source, lateral flatten(input => parse_json(jsontext), outer => true) d

    ),
    renamed as (

        select
            data_by_row['country']::varchar as country,
            data_by_row['gitlab']::varchar as gitlab_handle,
            data_by_row['gitlabId']::varchar as gitlab_id,
            data_by_row['isBackendMaintainer']::boolean as is_backend_maintainer,
            data_by_row[
                'isBackendTraineeMaintainer'
            ]::boolean as is_backend_trainee_maintainer,
            data_by_row['isDatabaseMaintainer']::boolean as is_database_maintainer,
            data_by_row[
                'isDatabaseTraineeMaintainer'
            ]::boolean as is_database_trainee_maintainer,
            data_by_row['isFrontendMaintainer']::boolean as is_frontend_maintainer,
            data_by_row[
                'isFrontendTraineeMaintainer'
            ]::boolean as is_frontend_trainee_maintainer,
            data_by_row['isManager']::boolean as is_manager,
            data_by_row['level']::varchar as team_member_level,
            data_by_row['locality']::varchar as locality,
            data_by_row['location_factor']::float as location_factor,
            data_by_row['matchName']::varchar as match_name,
            data_by_row['name']::varchar as name,
            data_by_row['section']::varchar as development_section,
            data_by_row['start_date']::date as start_date,
            data_by_row['team']::varchar as team,
            data_by_row['technology']::varchar as technology_group
        from flattened

    )
select *
from renamed
