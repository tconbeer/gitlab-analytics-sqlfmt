with
    labels as (select * from {{ ref("gitlab_dotcom_labels") }}),
    projects as (

        select project_id, visibility_level, namespace_id
        from {{ ref("gitlab_dotcom_projects") }}

    ),
    internal_namespaces as (

        select namespace_id
        from {{ ref("gitlab_dotcom_namespace_lineage") }}
        where namespace_is_internal

    ),
    joined as (

        select
            label_id,

            case
                when
                    projects.visibility_level != 'public' and namespace_id not in (
                        select * from internal_namespaces
                    )
                then 'content masked'
                else label_title
            end as masked_label_title,

            length(label_title) as title_length,
            color,
            labels.project_id,
            group_id,
            template,
            label_type,
            created_at as label_created_at,
            updated_at as label_updated_at

        from labels
        left join projects on labels.project_id = projects.project_id

    )

select *
from joined
