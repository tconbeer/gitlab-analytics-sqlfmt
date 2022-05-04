{{ config({"materialized": "incremental", "unique_key": "note_id"}) }}


{% set fields_to_mask = ["note"] %}

with
    base as (

        select *
        from {{ ref("gitlab_dotcom_notes") }}
        where
            noteable_type = 'Epic'
            {% if is_incremental() %}

            and updated_at >= (select max(updated_at) from {{ this }})

            {% endif %}
    )

    ,
    epics as (select * from {{ ref("gitlab_dotcom_epics_xf") }})

    ,
    namespaces as (select * from {{ ref("gitlab_dotcom_namespaces_xf") }})

    ,
    internal_namespaces as (

        select
            namespace_id,
            namespace_ultimate_parent_id,
            (
                namespace_ultimate_parent_id in {{ get_internal_parent_namespaces() }}
            ) as namespace_is_internal
        from {{ ref("gitlab_dotcom_namespaces_xf") }}

    )

    ,
    anonymised as (

        select
            {{ dbt_utils.star(from=ref('gitlab_dotcom_notes'), except=fields_to_mask|upper, relation_alias='base') }},
            {% for field in fields_to_mask %}
            case
                when
                    true
                    and namespaces.visibility_level != 'public'
                    and not internal_namespaces.namespace_is_internal
                then 'confidential - masked'
                else {{ field }}
            end as {{ field }},
            {% endfor %}
            epics.ultimate_parent_id
        from base
        left join epics on base.noteable_id = epics.epic_id
        left join namespaces on epics.group_id = namespaces.namespace_id
        left join
            internal_namespaces on epics.group_id = internal_namespaces.namespace_id

    )

select *
from anonymised
