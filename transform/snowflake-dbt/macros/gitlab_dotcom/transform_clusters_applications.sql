{%- macro transform_clusters_applications(base_model) -%}

    with
        base as (select * from {{ ref(base_model) }}),

        clusters as (select * from {{ ref("gitlab_dotcom_clusters_xf") }}),

        final as (

            select
                base.*,
                clusters.user_id,
                clusters.cluster_group_id,
                clusters.cluster_project_id,
                clusters.ultimate_parent_id
            from base
            inner join clusters on base.cluster_id = clusters.cluster_id

        )

    select *
    from final

{%- endmacro -%}
