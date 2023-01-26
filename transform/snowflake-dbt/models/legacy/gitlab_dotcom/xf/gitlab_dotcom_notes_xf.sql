{{ config({"materialized": "incremental", "unique_key": "note_id"}) }}


{% set fields_to_mask = ["note"] %}

with
    base as (

        select *
        from {{ ref("gitlab_dotcom_notes") }}
        {% if is_incremental() %}

        where updated_at >= (select max(updated_at) from {{ this }})

        {% endif %}
    ),
    projects as (select * from {{ ref("gitlab_dotcom_projects_xf") }}),
    internal_namespaces as (

        select
            namespace_id,
            namespace_ultimate_parent_id,
            (
                namespace_ultimate_parent_id in {{ get_internal_parent_namespaces() }}
            ) as namespace_is_internal
        from {{ ref("gitlab_dotcom_namespaces_xf") }}

    ),
    system_note_metadata as (

        select
            note_id,
            array_agg(action_type) within group (
                order by action_type asc
            ) as action_type_array
        from {{ ref("gitlab_dotcom_system_note_metadata") }}
        group by 1

    ),
    anonymised as (

        select
            {{
                dbt_utils.star(
                    from=ref("gitlab_dotcom_notes"),
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
            end as {{ field }},
            {% endfor %}
            projects.ultimate_parent_id,
            action_type_array
        from base
        left join projects on base.project_id = projects.project_id
        left join
            internal_namespaces
            on projects.namespace_id = internal_namespaces.namespace_id
        left join system_note_metadata on base.note_id = system_note_metadata.note_id

    )

select *
from anonymised
