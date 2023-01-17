with
    source as (select * from {{ ref("gitlab_dotcom_project_features_dedupe_source") }}),
    renamed as (

        select

            id::number as project_feature_id,
            project_id::number as project_id,
            merge_requests_access_level::number as merge_requests_access_level,
            issues_access_level::number as issues_access_level,
            wiki_access_level::number as wiki_access_level,
            snippets_access_level::number as snippets_access_level,
            builds_access_level::number as builds_access_level,
            repository_access_level::number as repository_access_level,
            created_at::timestamp as created_at,
            updated_at::timestamp as updated_at

        from source

    )

select *
from renamed
