with
    labels as (select * from {{ ref("gitlab_ops_labels") }}),
    projects as (

        select project_id, visibility_level, namespace_id
        from {{ ref("gitlab_ops_projects") }}

    ),
    joined as (

        select
            label_id,

            case
                -- AND namespace_id NOT IN (SELECT * FROM internal_namespaces) 
                when projects.visibility_level != 'public'
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
