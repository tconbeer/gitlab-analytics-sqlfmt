select
    'counts.incident_issues' as counter_name,
    count(issues.id) as counter_value,
    to_date(current_date) as run_day
from {{ ref("gitlab_dotcom_issues_dedupe_source") }} as issues
where issues.issue_type = 1
