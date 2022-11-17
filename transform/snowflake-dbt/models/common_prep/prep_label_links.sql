{{ config(tags=["product"]) }}

with
    gitlab_dotcom_label_links_source as (

        select * from {{ ref("gitlab_dotcom_label_links_source") }}

    ),
    renamed as (

        select
            gitlab_dotcom_label_links_source.label_link_id as dim_label_link_id,
            -- FOREIGN KEYS
            gitlab_dotcom_label_links_source.label_id as dim_label_id,
            -- foreign key to different table depending on target type of label
            case
                when gitlab_dotcom_label_links_source.target_type = 'Issue'
                then gitlab_dotcom_label_links_source.target_id
                else null
            end as dim_issue_id,
            case
                when gitlab_dotcom_label_links_source.target_type = 'MergeRequest'
                then gitlab_dotcom_label_links_source.target_id
                else null
            end as dim_merge_request_id,
            case
                when gitlab_dotcom_label_links_source.target_type = 'Epic'
                then gitlab_dotcom_label_links_source.target_id
                else null
            end as dim_epic_id,
            --
            gitlab_dotcom_label_links_source.target_type,
            gitlab_dotcom_label_links_source.label_link_created_at as label_added_at,
            gitlab_dotcom_label_links_source.label_link_updated_at as label_updated_at
        --
        from gitlab_dotcom_label_links_source
        -- exclude broken links (deleted labels)
        where
            gitlab_dotcom_label_links_source.label_id is not null
            -- only include currently active labels to avoid duplicate label_link_ids
            and is_currently_valid = true


    )

    {{
        dbt_audit(
            cte_ref="renamed",
            created_by="@dtownsend",
            updated_by="@dtownsend",
            created_date="2021-08-04",
            updated_date="2021-08-04",
        )
    }}
