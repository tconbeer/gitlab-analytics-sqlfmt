with
    clusters as (select * from {{ ref("gitlab_dotcom_clusters") }}),

    cluster_groups as (select * from {{ ref("gitlab_dotcom_cluster_groups") }}),

    cluster_projects as (select * from {{ ref("gitlab_dotcom_cluster_projects") }}),

    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces_xf") }}),

    projects as (select * from {{ ref("gitlab_dotcom_projects_xf") }}),

    final as (

        select
            clusters.*,
            cluster_groups.cluster_group_id,
            cluster_projects.cluster_project_id,
            coalesce(
                namespaces.namespace_ultimate_parent_id, projects.ultimate_parent_id
            ) as ultimate_parent_id
        from clusters
        left join cluster_groups on clusters.cluster_id = cluster_groups.cluster_id
        left join cluster_projects on clusters.cluster_id = cluster_projects.cluster_id
        left join
            namespaces on cluster_groups.cluster_group_id = namespaces.namespace_id
        left join projects on cluster_projects.cluster_project_id = projects.project_id

    )

select *
from final
