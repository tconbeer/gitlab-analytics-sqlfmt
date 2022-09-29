{{ config(materialized="ephemeral") }}

with
    namespaces as (select * from {{ ref("dim_namespace") }}),
    namespace_path as (

        select
            namespaces.dim_namespace_id,
            case
                when namespace_4.dim_namespace_id is not null
                then
                    namespace_4.namespace_path
                    || '/'
                    || namespace_3.namespace_path
                    || '/'
                    || namespace_2.namespace_path
                    || '/'
                    || namespace_1.namespace_path
                    || '/'
                    || namespaces.namespace_path
                when namespace_3.dim_namespace_id is not null
                then
                    namespace_3.namespace_path
                    || '/'
                    || namespace_2.namespace_path
                    || '/'
                    || namespace_1.namespace_path
                    || '/'
                    || namespaces.namespace_path
                when namespace_2.dim_namespace_id is not null
                then
                    namespace_2.namespace_path
                    || '/'
                    || namespace_1.namespace_path
                    || '/'
                    || namespaces.namespace_path
                when namespace_1.dim_namespace_id is not null
                then namespace_1.namespace_path || '/' || namespaces.namespace_path
                else namespaces.namespace_path
            end as full_namespace_path
        from namespaces
        left outer join
            namespaces namespace_1
            on namespace_1.dim_namespace_id = namespaces.parent_id
            and namespaces.namespace_is_ultimate_parent = false
        left outer join
            namespaces namespace_2
            on namespace_2.dim_namespace_id = namespace_1.parent_id
            and namespace_1.namespace_is_ultimate_parent = false
        left outer join
            namespaces namespace_3
            on namespace_3.dim_namespace_id = namespace_2.parent_id
            and namespace_2.namespace_is_ultimate_parent = false
        left outer join
            namespaces namespace_4
            on namespace_4.dim_namespace_id = namespace_3.parent_id
            and namespace_3.namespace_is_ultimate_parent = false

    )

select *
from namespace_path
