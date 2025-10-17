with
    source as (

        select *
        from {{ ref("gitlab_dotcom_clusters_applications_cert_managers_source") }}

    )

select *
from source
