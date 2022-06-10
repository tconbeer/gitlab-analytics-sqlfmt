with
    salesforce_date as (

        {% set tables = [
    "account",
    "campaign",
    "campaign_member",
    "contact",
    "event",
    "executive_business_review",
    "lead",
    "opportunity_stage",
    "opportunity_split",
    "opportunity_split_type",
    "opportunity_team_member",
    "opportunity",
    "opportunity_contact_role",
    "proof_of_concept",
    "record_type",
    "statement_of_work",
    "task",
    "user_role",
    "user",
    "zqu_quote",
] %}

        {% for table in tables %}
        select '{{table}}' as table_name, max(lastmodifieddate) as max_date
        from {{ source("salesforce", table) }}


        {% if not loop.last %} UNION ALL {% endif %}

        {% endfor %}

        UNION ALL

        {% set tables = [
    "account_history",
    "contact_history",
    "lead_history",
    "opportunity_field_history",
    "opportunity_history",
] %}

        {% for table in tables %}
        select '{{table}}' as table_name, max(createddate) as max_date
        from {{ source("salesforce", table) }}


        {% if not loop.last %} UNION ALL {% endif %}

        {% endfor %}

    )

select *
from salesforce_date
