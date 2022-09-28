{{ config(tags=["mnpi_exception"]) }}

{{ config({"schema": "common_mart_marketing"}) }}

with
    mart_marketing_contact as (

        select
            {{
                dbt_utils.star(
                    from=ref("mart_marketing_contact"),
                    except=[
                        "EMAIL_ADDRESS",
                        "FIRST_NAME",
                        "LAST_NAME",
                        "GITLAB_USER_NAME",
                        "GITLAB_DOTCOM_USER_ID",
                        "MOBILE_PHONE",
                        "PQL_NAMESPACE_NAME",
                        "CREATED_BY",
                        "UPDATED_BY",
                        "MODEL_CREATED_DATE",
                        "MODEL_UPDATED_DATE",
                        "DBT_UPDATED_AT",
                        "DBT_CREATED_AT",
                    ],
                )
            }}
        from {{ ref("mart_marketing_contact") }}

    )

    {{
        dbt_audit(
            cte_ref="mart_marketing_contact",
            created_by="@jpeguero",
            updated_by="@jpeguero",
            created_date="2021-05-13",
            updated_date="2022-01-10",
        )
    }}
