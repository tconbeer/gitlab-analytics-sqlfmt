with
    sfdc_account as (select * from {{ ref("sfdc_account") }}),
    sfdc_users as (select * from {{ ref("sfdc_users") }}),
    sfdc_record_type as (select * from {{ ref("sfdc_record_type") }}),
    sfdc_account_deal_size_segmentation as (

        select * from {{ ref("sfdc_account_deal_size_segmentation") }}

    ),
    parent_account as (select * from {{ ref("sfdc_account") }}),
    joined as (

        select
            sfdc_account.*,

            tam_user.name as technical_account_manager,
            parent_account.account_name as ultimate_parent_account_name,

            -- ************************************
            -- sales segmentation deprecated fields - 2020-09-03
            -- left temporary for the sake of MVC and avoid breaking SiSense existing
            -- charts
            -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/5709
            sfdc_account.ultimate_parent_sales_segment
            as ultimate_parent_account_segment,
            -- ************************************
            sfdc_record_type.record_type_name,
            sfdc_record_type.business_process_id,
            sfdc_record_type.record_type_label,
            sfdc_record_type.record_type_description,
            sfdc_record_type.record_type_modifying_object_type,
            sfdc_account_deal_size_segmentation.deal_size,
            case
                when
                    sfdc_account.ultimate_parent_sales_segment in (
                        'Large', 'Strategic'
                    ) or sfdc_account.division_sales_segment in ('Large', 'Strategic')
                then true
                else false
            end as is_large_and_up,


            -- NF 20210829 Zoom info technology flags
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: Jenkins')
                then 1
                else 0
            end as zi_jenkins_presence_flag,
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: SVN')
                then 1
                else 0
            end as zi_svn_presence_flag,
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: Tortoise SVN')
                then 1
                else 0
            end as zi_tortoise_svn_presence_flag,
            case
                when
                    contains(
                        sfdc_account.zi_technologies, 'ARE_USED: Google Cloud Platform'
                    )
                then 1
                else 0
            end as zi_gcp_presence_flag,
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: Atlassian')
                then 1
                else 0
            end as zi_atlassian_presence_flag,
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: GitHub')
                then 1
                else 0
            end as zi_github_presence_flag,
            case
                when
                    contains(
                        sfdc_account.zi_technologies, 'ARE_USED: GitHub Enterprise'
                    )
                then 1
                else 0
            end as zi_github_enterprise_presence_flag,
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: AWS')
                then 1
                else 0
            end as zi_aws_presence_flag,
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: Kubernetes')
                then 1
                else 0
            end as zi_kubernetes_presence_flag,
            case
                when
                    contains(
                        sfdc_account.zi_technologies, 'ARE_USED: Apache Subversion'
                    )
                then 1
                else 0
            end as zi_apache_subversion_presence_flag,
            case
                when
                    contains(
                        sfdc_account.zi_technologies,
                        'ARE_USED: Apache Subversion (SVN)'
                    )
                then 1
                else 0
            end as zi_apache_subversion_svn_presence_flag,
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: Hashicorp')
                then 1
                else 0
            end as zi_hashicorp_presence_flag,
            case
                when
                    contains(
                        sfdc_account.zi_technologies, 'ARE_USED: Amazon AWS CloudTrail'
                    )
                then 1
                else 0
            end as zi_aws_cloud_trail_presence_flag,
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: CircleCI')
                then 1
                else 0
            end as zi_circle_ci_presence_flag,
            case
                when contains(sfdc_account.zi_technologies, 'ARE_USED: BitBucket')
                then 1
                else 0
            end as zi_bit_bucket_presence_flag,

            -- NF 2022-01-28 Added extra account owner demographics fields
            -- account_owner.user_segment            AS account_owner_user_segment, --
            -- coming directly from source table
            account_owner.user_geo as account_owner_user_geo,
            account_owner.user_region as account_owner_user_region,
            account_owner.user_area as account_owner_user_area,

            parent_account.account_demographics_sales_segment
            as upa_demographics_segment,
            parent_account.account_demographics_geo as upa_demographics_geo,
            parent_account.account_demographics_region as upa_demographics_region,
            parent_account.account_demographics_area as upa_demographics_area,
            parent_account.account_demographics_territory as upa_demographics_territory

        from sfdc_account
        left join
            parent_account
            on sfdc_account.ultimate_parent_account_id = parent_account.account_id
        left join
            sfdc_users tam_user
            on sfdc_account.technical_account_manager_id = tam_user.user_id
        left join
            sfdc_users account_owner on sfdc_account.owner_id = account_owner.user_id
        left join
            sfdc_record_type
            on sfdc_account.record_type_id = sfdc_record_type.record_type_id
        left join
            sfdc_account_deal_size_segmentation
            on sfdc_account.account_id = sfdc_account_deal_size_segmentation.account_id

    )

select *
from joined
