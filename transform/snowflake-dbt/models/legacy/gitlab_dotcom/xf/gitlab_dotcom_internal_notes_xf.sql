with
    notes as (

        select
            {{
                dbt_utils.star(
                    from=ref("gitlab_dotcom_notes"),
                    except=["created_at", "updated_at"],
                )
            }},
            created_at as note_created_at,
            updated_at as note_updated_at,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                note, '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}'
            ) as sfdc_link_array,
            "{{this.database}}".{{ target.schema }}.regexp_to_array(
                note, '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}'
            ) as zendesk_link_array
        from {{ ref("gitlab_dotcom_notes") }}
        where noteable_type in ('Epic', 'Issue', 'MergeRequest')

    )

    ,
    projects as (select * from {{ ref("gitlab_dotcom_projects_xf") }})

    ,
    epics as (select * from {{ ref("gitlab_dotcom_epics_xf") }})

    ,
    internal_namespaces as (

        select *
        from {{ ref("gitlab_dotcom_namespaces_xf") }}
        where namespace_is_internal

    )

    ,
    notes_with_namespaces as (

        select
            notes.*,
            projects.project_name,
            internal_namespaces.namespace_id,
            internal_namespaces.namespace_name

        from notes
        left join
            projects on notes.noteable_type in (
                'Issue', 'MergeRequest'
            ) and notes.project_id = projects.project_id
        left join
            epics on notes.noteable_type = 'Epic' and notes.noteable_id = epics.epic_id
        inner join
            internal_namespaces on coalesce(
                projects.ultimate_parent_id, epics.ultimate_parent_id
            ) = internal_namespaces.namespace_id

    )

select *
from notes_with_namespaces
