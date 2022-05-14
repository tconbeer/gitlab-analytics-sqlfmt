select
    namespace_id,
    to_date(current_date) as run_day,
    count(distinct gitlab_dotcom_projects_dedupe_source.creator_id) as counter_value
from {{ ref("gitlab_dotcom_projects_dedupe_source") }}
left join
    {{ ref("gitlab_dotcom_namespaces_dedupe_source") }}
    on
    gitlab_dotcom_namespaces_dedupe_source.id
    = gitlab_dotcom_projects_dedupe_source.namespace_id
where
    gitlab_dotcom_projects_dedupe_source.import_type = 'fogbugz'
    and gitlab_dotcom_projects_dedupe_source.import_type is not null
group by 1
