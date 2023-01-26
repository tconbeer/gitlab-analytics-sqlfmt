with
    bizible_touchpoints as (

        select
            touchpoint_id,
            campaign_id,
            bizible_touchpoint_type,
            bizible_touchpoint_source,
            bizible_landing_page,
            bizible_landing_page_raw,
            bizible_referrer_page,
            bizible_referrer_page_raw,
            bizible_form_url,
            bizible_form_url_raw,
            bizible_ad_campaign_name,
            bizible_marketing_channel_path,
            bizible_medium,
            bizible_ad_content
        from {{ ref("sfdc_bizible_touchpoint_source") }}
        where is_deleted = 'FALSE'

    ),
    bizible_attribution_touchpoints as (

        select
            touchpoint_id,
            campaign_id,
            bizible_touchpoint_type,
            bizible_touchpoint_source,
            bizible_landing_page,
            bizible_landing_page_raw,
            bizible_referrer_page,
            bizible_referrer_page_raw,
            bizible_form_url,
            bizible_form_url_raw,
            bizible_ad_campaign_name,
            bizible_marketing_channel_path,
            bizible_medium,
            bizible_ad_content
        from {{ ref("sfdc_bizible_attribution_touchpoint_source") }}
        where is_deleted = 'FALSE'

    ),
    bizible as (

        select *
        from bizible_touchpoints

        union all

        select *
        from bizible_attribution_touchpoints

    ),
    campaign as (select * from {{ ref("prep_campaign") }}),
    touchpoints_with_campaign as (

        select
            {{
                dbt_utils.surrogate_key(
                    [
                        "campaign.dim_campaign_id",
                        "campaign.dim_parent_campaign_id",
                        "bizible.bizible_touchpoint_type",
                        "bizible.bizible_landing_page",
                        "bizible.bizible_referrer_page",
                        "bizible.bizible_form_url",
                        "bizible.bizible_ad_campaign_name",
                        "bizible.bizible_marketing_channel_path",
                    ]
                )
            }} as bizible_campaign_grouping_id,
            bizible.touchpoint_id as dim_crm_touchpoint_id,
            campaign.dim_campaign_id,
            campaign.dim_parent_campaign_id,
            bizible.bizible_touchpoint_type,
            bizible.bizible_touchpoint_source,
            bizible.bizible_landing_page,
            bizible.bizible_landing_page_raw,
            bizible.bizible_referrer_page,
            bizible.bizible_referrer_page_raw,
            bizible.bizible_form_url,
            bizible.bizible_ad_campaign_name,
            bizible.bizible_marketing_channel_path,
            bizible.bizible_ad_content,
            bizible.bizible_medium,
            bizible.bizible_form_url_raw,
            case
                when
                    -- based on issue
                    -- https://gitlab.com/gitlab-com/marketing/marketing-strategy-performance/-/issues/246
                    dim_parent_campaign_id = '7014M000001dowZQAQ'
                    or (
                        bizible_medium = 'sponsorship'
                        and bizible_touchpoint_source in (
                            'issa',
                            'stackoverflow',
                            'securityweekly-appsec',
                            'unix&linux',
                            'stackexchange'
                        )
                    )
                then 'Publishers/Sponsorships'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%smbnurture%'
                            or bizible_form_url like '%smbnurture%'
                            or bizible_referrer_page like '%smbnurture%'
                            or bizible_ad_campaign_name like '%smbnurture%'
                            or bizible_landing_page like '%smbagnostic%'
                            or bizible_form_url like '%smbagnostic%'
                            or bizible_referrer_page like '%smbagnostic%'
                            or bizible_ad_campaign_name like '%smbagnostic%'
                        )
                    )
                    or bizible_ad_campaign_name = 'Nurture - SMB Mixed Use Case'
                then 'SMB Nurture'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%cicdseeingisbelieving%'
                            or bizible_form_url like '%cicdseeingisbelieving%'
                            or bizible_referrer_page like '%cicdseeingisbelieving%'
                            or bizible_ad_campaign_name like '%cicdseeingisbelieving%'
                        )
                    )
                    or dim_parent_campaign_id = '7014M000001dmNAQAY'
                then 'CI/CD Seeing is Believing'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%simplifydevops%'
                            or bizible_form_url like '%simplifydevops%'
                            or bizible_referrer_page like '%simplifydevops%'
                            or bizible_ad_campaign_name like '%simplifydevops%'
                        )
                    )
                    or dim_parent_campaign_id = '7014M000001doAGQAY'
                    or dim_campaign_id like '7014M000001dn6z%'
                then 'Simplify DevOps'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%21q4-jp%'
                            or bizible_form_url like '%21q4-jp%'
                            or bizible_referrer_page like '%21q4-jp%'
                            or bizible_ad_campaign_name like '%21q4-jp%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and bizible_ad_campaign_name
                        = '2021_Social_Japan_LinkedIn Lead Gen'
                    )
                then 'Japan-Digital Readiness'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%lower-tco%'
                            or bizible_form_url like '%lower-tco%'
                            or bizible_referrer_page like '%lower-tco%'
                            or bizible_ad_campaign_name like '%operationalefficiencies%'
                            or bizible_ad_campaign_name like '%operationalefficiences%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and (
                            bizible_ad_campaign_name like '%_Operational Efficiencies%'
                            or bizible_ad_campaign_name like '%operationalefficiencies%'
                        )
                    )
                then 'Increase Operational Efficiencies'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%reduce-cycle-time%'
                            or bizible_form_url like '%reduce-cycle-time%'
                            or bizible_referrer_page like '%reduce-cycle-time%'
                            or bizible_ad_campaign_name like '%betterproductsfaster%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and (
                            bizible_ad_campaign_name like '%_Better Products Faster%'
                            or bizible_ad_campaign_name like '%betterproductsfaster%'
                        )
                    )
                then 'Deliver Better Products Faster'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%secure-apps%'
                            or bizible_form_url like '%secure-apps%'
                            or bizible_referrer_page like '%secure-apps%'
                            or bizible_ad_campaign_name like '%reducesecurityrisk%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and (
                            bizible_ad_campaign_name like '%_Reduce Security Risk%'
                            or bizible_ad_campaign_name like '%reducesecurityrisk%'
                        )
                    )
                then 'Reduce Security and Compliance Risk'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%jenkins-alternative%'
                            or bizible_form_url like '%jenkins-alternative%'
                            or bizible_referrer_page like '%jenkins-alternative%'
                            or bizible_ad_campaign_name like '%cicdcmp2%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and (
                            bizible_ad_campaign_name like '%_Jenkins%'
                            or bizible_ad_campaign_name like '%cicdcmp2%'
                        )
                    )
                then 'Jenkins Take Out'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%single-application-ci%'
                            or bizible_form_url like '%single-application-ci%'
                            or bizible_referrer_page like '%single-application-ci%'
                            or bizible_ad_campaign_name like '%cicdcmp3%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and bizible_ad_campaign_name like '%cicdcmp3%'
                    )
                then 'CI Build & Test Auto'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%github-actions-alternative%'
                            or bizible_form_url like '%github-actions-alternative%'
                            or bizible_referrer_page like '%github-actions-alternative%'
                            or bizible_ad_campaign_name like '%octocat%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and bizible_ad_campaign_name ilike '%_OctoCat%'
                    )
                then 'OctoCat'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page
                            like '%integration-continue-pour-construire-et-tester-plus-rapidement%'
                            or bizible_form_url
                            like '%integration-continue-pour-construire-et-tester-plus-rapidement%'
                            or bizible_referrer_page
                            like '%integration-continue-pour-construire-et-tester-plus-rapidement%'
                            or (
                                bizible_ad_campaign_name like '%singleappci%'
                                and bizible_ad_content like '%french%'
                            )
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and bizible_ad_campaign_name ilike '%Singleappci_French%'
                    )
                then 'CI Use Case - FR'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page
                            like '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
                            or bizible_form_url
                            like '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
                            or bizible_referrer_page
                            like '%nutze-continuous-integration-fuer-schnelleres-bauen-und-testen%'
                            or (
                                bizible_ad_campaign_name like '%singleappci%'
                                and bizible_ad_content like '%paesslergerman%'
                            )
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and bizible_ad_campaign_name ilike '%Singleappci_German%'
                    )
                then 'CI Use Case - DE'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page
                            like '%use-continuous-integration-to-build-and-test-faster%'
                            or bizible_form_url
                            like '%use-continuous-integration-to-build-and-test-faster%'
                            or bizible_referrer_page
                            like '%use-continuous-integration-to-build-and-test-faster%'
                            or bizible_ad_campaign_name like '%singleappci%'
                        )
                    )
                    or bizible_ad_campaign_name
                    = '20201013_ActualTechMedia_DeepMonitoringCI'
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and (
                            bizible_ad_campaign_name like '%_CI%'
                            or bizible_ad_campaign_name ilike '%singleappci%'
                        )
                    )
                then 'CI Use Case'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page
                            like '%shift-your-security-scanning-left%'
                            or bizible_form_url
                            like '%shift-your-security-scanning-left%'
                            or bizible_referrer_page
                            like '%shift-your-security-scanning-left%'
                            or bizible_ad_campaign_name like '%devsecopsusecase%'
                        )
                    )
                    -- GCP Partner campaign
                    or dim_parent_campaign_id = '7014M000001dnVOQAY'
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and (
                            bizible_ad_campaign_name ilike '%_DevSecOps%'
                            or bizible_ad_campaign_name like '%devsecopsusecase%'
                        )
                    )
                then 'DevSecOps Use Case'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%aws-gitlab-serverless%'
                            or bizible_landing_page like '%trek10-aws-cicd%'
                            or bizible_form_url like '%aws-gitlab-serverless%'
                            or bizible_form_url like '%trek10-aws-cicd%'
                            or bizible_referrer_page like '%aws-gitlab-serverless%'
                            or bizible_ad_campaign_name like '%awspartner%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and bizible_ad_campaign_name ilike '%_AWS%'
                    )
                then 'AWS'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page
                            like '%simplify-collaboration-with-version-control%'
                            or bizible_form_url
                            like '%simplify-collaboration-with-version-control%'
                            or bizible_referrer_page
                            like '%simplify-collaboration-with-version-control%'
                            or bizible_ad_campaign_name like '%vccusecase%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and (
                            bizible_ad_campaign_name like '%_VCC%'
                            or bizible_ad_campaign_name like '%vccusecase%'
                        )
                    )
                then 'VCC Use Case'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page
                            like '%gitops-infrastructure-automation%'
                            or bizible_form_url
                            like '%gitops-infrastructure-automation%'
                            or bizible_referrer_page
                            like '%gitops-infrastructure-automation%'
                            or bizible_ad_campaign_name like '%iacgitops%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and (
                            bizible_ad_campaign_name like '%_GitOps%'
                            or bizible_ad_campaign_name like '%iacgitops%'
                        )
                    )
                then 'GitOps Use Case'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_ad_campaign_name like '%evergreen%'
                            or bizible_form_url_raw like '%utm_campaign=evergreen%'
                            or bizible_landing_page_raw like '%utm_campaign=evergreen%'
                            or bizible_referrer_page_raw like '%utm_campaign=evergreen%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and bizible_ad_campaign_name ilike '%_Evergreen%'
                    )
                then 'Evergreen'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_ad_campaign_name like 'brand%'
                            or bizible_ad_campaign_name like 'Brand%'
                            or bizible_form_url_raw like '%utm_campaign=brand%'
                            or bizible_landing_page_raw like '%utm_campaign=brand%'
                            or bizible_referrer_page_raw like '%utm_campaign=brand%'
                        )
                    )
                    or (
                        dim_parent_campaign_id = '7014M000001dn8MQAQ'
                        and bizible_ad_campaign_name ilike '%_Brand%'
                    )
                then 'Brand'
                when
                    (
                        -- added 2021-06-04 MSandP: 332
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_landing_page like '%contact-us-ultimate%'
                            or bizible_form_url like '%contact-us-ultimate%'
                            or bizible_referrer_page like '%contact-us-ultimate%'
                            or bizible_ad_campaign_name like '%premtoultimatesp%'
                        )
                    )
                then 'Premium to Ultimate'
                when
                    (
                        -- added 2021-06-04 MSandP: 346
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_form_url_raw like '%webcast-gitops-multicloudapp%'
                            or bizible_landing_page_raw
                            like '%webcast-gitops-multicloudapp%'
                            or bizible_referrer_page_raw
                            like '%webcast-gitops-multicloudapp%'
                        )
                    )
                    or (dim_parent_campaign_id like '%7014M000001dpmf%')
                then 'GitOps GTM webcast'
                when
                    (
                        -- added 2021-06-04 MSandP: 346
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            bizible_form_url_raw like '%devopsgtm%'
                            or bizible_landing_page_raw like '%devopsgtm%'
                            or bizible_referrer_page_raw like '%devopsgtm%'
                        )
                    )
                    or dim_parent_campaign_id like '%7014M000001dpT9%'
                    -- OR camp.campaign_parent_id LIKE '%7014M000001dn8M%')
                    or dim_campaign_id like '%7014M000001vbtw%'
                then 'DevOps GTM'

                when
                    (
                        -- added 2021-06-04 MSandP: 346
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            (
                                bizible_form_url_raw like '%utm_campaign=devopsgtm%'
                                and bizible_form_url_raw
                                like '%utm_content=partnercredit%'
                                or bizible_landing_page_raw
                                like '%utm_campaign=devopsgtm%'
                                and bizible_landing_page_raw
                                like '%utm_content=partnercredit%'
                                or bizible_referrer_page_raw
                                like '%utm_campaign=devopsgtm%'
                                and bizible_referrer_page_raw
                                like '%utm_content=partnercredit%'
                            )
                            or (
                                bizible_form_url_raw like '%cloud-credits-promo%'
                                or bizible_landing_page_raw like '%cloud-credits-promo%'
                                or bizible_referrer_page_raw
                                like '%cloud-credits-promo%'
                            )
                        )
                    )
                    or dim_parent_campaign_id like '%7014M000001vcDr%'
                    or dim_campaign_id like '%7014M000001vcDr%'
                then 'Cloud Partner Campaign'
                when
                    (
                        -- added 2021-06-04 MSandP: 346
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            (
                                bizible_form_url_raw like '%utm_campaign=gitlab14%'
                                or bizible_landing_page_raw
                                like '%utm_campaign=gitlab14%'
                                or bizible_referrer_page_raw
                                like '%utm_campaign=gitlab14%'
                            )
                            or (
                                bizible_form_url_raw like '%the-shift-to-modern-devops%'
                                or bizible_landing_page_raw
                                like '%the-shift-to-modern-devops%'
                                or bizible_referrer_page_raw
                                like '%the-shift-to-modern-devops%'
                            )
                        )
                    )
                then 'GitLab 14 webcast'
                when dim_campaign_id like '%7014M000001drcQ%'
                then '20210512_ISSAWebcast'
                when
                    (
                        -- added 2021-08-30 MSandP: 325
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            (
                                bizible_form_url_raw like '%psdigitaltransformation%'
                                or bizible_landing_page_raw
                                like '%psdigitaltransformation%'
                                or bizible_referrer_page_raw
                                like '%psdigitaltransformation%'
                            )
                            or (
                                bizible_form_url_raw like '%psglobal%'
                                or bizible_landing_page_raw like '%psglobal%'
                                or bizible_referrer_page_raw like '%psglobal%'
                            )
                        )
                    )
                then 'PubSec Nurture'
                when
                    (
                        -- added 2021-11-22 MSandP: 585
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            (
                                bizible_form_url_raw like '%whygitlabdevopsplatform%'
                                or bizible_landing_page_raw
                                like '%whygitlabdevopsplatform%'
                                or bizible_referrer_page_raw
                                like '%whygitlabdevopsplatform%'
                            )
                            or (
                                bizible_form_url_raw like '%githubcompete%'
                                or bizible_landing_page_raw like '%githubcompete%'
                                or bizible_referrer_page_raw like '%githubcompete%'
                            )
                        )
                    )
                then 'FY22 GitHub Competitive Campaign'
                when
                    (
                        -- added 2021-11-22 MSandP: 570
                        bizible_touchpoint_type = 'Web Form'
                        and (
                            (
                                bizible_form_url_raw like '%devopsgtm%'
                                or bizible_landing_page_raw like '%devopsgtm%'
                                or bizible_referrer_page_raw like '%devopsgtm%'
                            )
                        )
                    )
                    or dim_campaign_id like '%7014M000001dqb2%'
                then 'DOI Webcast'
                when
                    (
                        bizible_touchpoint_type = 'Web Form'  -- MSandP 657
                        and (
                            bizible_form_url_raw like '%utm_campaign=cdusecase%'
                            or bizible_landing_page_raw like '%utm_campaign=cdusecase%'
                            or bizible_referrer_page_raw like '%utm_campaign=cdusecase%'
                        )
                    )
                then 'CD Use Case'
                else 'None'
            end as bizible_integrated_campaign_grouping,
            case
                when
                    bizible_integrated_campaign_grouping in (
                        'CI Build & Test Auto',
                        'CI Use Case',
                        'CI Use Case - FR',
                        'CI Use Case - DE',
                        'CI/CD Seeing is Believing',
                        'Jenkins Take Out',
                        'OctoCat',
                        'Premium to Ultimate',
                        '20210512_ISSAWebcast'
                    )
                then 'CI/CD'
                when
                    dim_parent_campaign_id = '7014M000001vm9KQAQ'
                    -- override for TechDemo Series
                    and campaign.gtm_motion = 'CI (CI/CD)'
                then 'CI/CD'
                when
                    bizible_integrated_campaign_grouping in (
                        'Deliver Better Products Faster',
                        'DevSecOps Use Case',
                        'Reduce Security and Compliance Risk',
                        'Simplify DevOps',
                        'DevOps GTM',
                        'Cloud Partner Campaign',
                        'GitLab 14 webcast',
                        'DOI Webcast',
                        'FY22 GitHub Competitive Campaign'
                    )
                then 'DevOps'
                when
                    dim_parent_campaign_id = '7014M000001vm9KQAQ'
                    -- override for TechDemo Series
                    and campaign.gtm_motion = 'DevOps Platform'
                then 'DevOps'
                when
                    bizible_integrated_campaign_grouping
                    in ('GitOps Use Case', 'GitOps GTM webcast')
                then 'GitOps'
                when
                    dim_parent_campaign_id = '7014M000001vm9KQAQ'
                    and campaign.gtm_motion = 'GITOPS'  -- override for TechDemo Series
                then 'GitOps'
                else null
            end as gtm_motion,
            iff(
                bizible_integrated_campaign_grouping <> 'None'
                or dim_parent_campaign_id = '7014M000001vm9KQAQ',
                'Demand Gen',
                'Other'
            )  -- override for TechDemo Series
            as touchpoint_segment,
            case
                -- Specific touchpoint overrides
                when touchpoint_id ilike 'a6061000000CeS0%'
                then 'Field Event'
                when bizible_marketing_channel_path = 'CPC.AdWords'
                then 'Google AdWords'
                when
                    bizible_marketing_channel_path
                    in ('Email.Other', 'Email.Newsletter', 'Email.Outreach')
                then 'Email'
                when
                    bizible_marketing_channel_path in (
                        'Field Event',
                        'Partners.Google',
                        'Brand.Corporate Event',
                        'Conference',
                        'Speaking Session'
                    )
                    or (
                        bizible_medium = 'Field Event (old)'
                        and bizible_marketing_channel_path = 'Other'
                    )
                then 'Field Event'
                when
                    bizible_marketing_channel_path in (
                        'Paid Social.Facebook',
                        'Paid Social.LinkedIn',
                        'Paid Social.Twitter',
                        'Paid Social.YouTube'
                    )
                then 'Paid Social'
                when
                    bizible_marketing_channel_path in (
                        'Social.Facebook',
                        'Social.LinkedIn',
                        'Social.Twitter',
                        'Social.YouTube'
                    )
                then 'Social'
                when
                    bizible_marketing_channel_path
                    in ('Marketing Site.Web Referral', 'Web Referral')
                then 'Web Referral'
                when
                    bizible_marketing_channel_path
                    -- Added to Web Direct
                    in ('Marketing Site.Web Direct', 'Web Direct')
                    or dim_campaign_id in (
                        '701610000008ciRAAQ',  -- Trial - GitLab.com
                        '70161000000VwZbAAK',  -- Trial - Self-Managed
                        '70161000000VwZgAAK',  -- Trial - SaaS
                        '70161000000CnSLAA0',  -- 20181218_DevOpsVirtual
                        '701610000008cDYAAY'  -- 2018_MovingToGitLab
                    )
                then 'Web Direct'
                when
                    bizible_marketing_channel_path like 'Organic Search.%'
                    or bizible_marketing_channel_path = 'Marketing Site.Organic'
                then 'Organic Search'
                when bizible_marketing_channel_path in ('Sponsorship')
                then 'Paid Sponsorship'
                else 'Unknown'
            end as integrated_campaign_grouping

        from bizible
        left join campaign on bizible.campaign_id = campaign.dim_campaign_id

    )

    {{
        dbt_audit(
            cte_ref="touchpoints_with_campaign",
            created_by="@mcooperDD",
            updated_by="@degan",
            created_date="2021-03-02",
            updated_date="2021-12-20",
        )
    }}
