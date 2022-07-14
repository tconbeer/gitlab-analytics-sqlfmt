{% set fields_to_mask = ["environment_name", "external_url", "slug"] %}

with
    base as (select * from {{ ref("gitlab_dotcom_environments") }}),
    projects as (select * from {{ ref("gitlab_dotcom_projects_xf") }}),
    internal_namespaces as (

        select namespace_id, namespace_is_internal
        from {{ ref("gitlab_dotcom_namespaces_xf") }}

    ),
    anonymised as (

        select
            {{
                dbt_utils.star(
                    from=ref("gitlab_dotcom_environments"),
                    except=fields_to_mask | upper,
                    relation_alias="base",
                )
            }},
            {% for field in fields_to_mask %}
            case
                when
                    true
                    and projects.visibility_level != 'public'
                    and not internal_namespaces.namespace_is_internal
                then 'confidential - masked'
                else {{ field }}
            end as {{ field }}
            {% if not loop.last %}, {% endif %}
            {% endfor %}
        from base
        left join projects on base.project_id = projects.project_id
        left join
            internal_namespaces
            on projects.namespace_id = internal_namespaces.namespace_id

    )

select *
from anonymised
