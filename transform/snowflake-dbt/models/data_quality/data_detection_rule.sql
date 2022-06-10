with
    detection_rule as (

        select
            1 as rule_id,
            'Missing instance types' as rule_name,
            'Missing instance types for UUID or Namespaces' as rule_description,
            'Product' as type_of_data,
            50 as threshold

        UNION

        select
            2 as rule_id,
            'Licenses with missing Subscriptions' as rule_name,
            'License IDs that do not have an associated Subscription ID'
            as rule_description,
            'Product' as type_of_data,
            50 as threshold

        UNION

        select
            3 as rule_id,
            'Subscription with paying Self-Managed Plans with missing Licenses'
            as rule_name,
            'Subscription Names that currently do not have an associated License ID'
            as rule_description,
            'Product' as type_of_data,
            50 as threshold

        UNION

        select
            4 as rule_id,
            'Subscriptions with Self-Managed Plans having License Start dates in the future'
            as rule_name,
            'Subscription IDs with Self-Managed Plans having license_start_date in the future'
            as rule_description,
            'Product' as type_of_data,
            50 as threshold

        UNION

        select
            5 as rule_id,
            'Subscriptions with Self-Managed Plans having License Start Date greater than License Expire date'
            as rule_name,
            'Subscriptions IDs with Self-Managed Plans having license_start_date greater than license_expire_date'
            as rule_description,
            'Product' as type_of_data,
            50 as threshold

        UNION

        select
            6 as rule_id,
            'Expired Licenses with Subscription End Dates in the Past' as rule_name,
            'Expired License IDs with Subscription End Dates in the Past'
            as rule_description,
            'Product' as type_of_data,
            50 as threshold

        UNION

        select
            7 as rule_id,
            'Active/Paid SaaS Subscriptions Not Mapped to Namespaces' as rule_name,
            'Currently paying SaaS Subscription IDs that do not have any associated Namespace IDs'
            as rule_description,
            'Product' as type_of_data,
            50 as threshold

    )

    {{
        dbt_audit(
            cte_ref="detection_rule",
            created_by="@snalamaru",
            updated_by="@jpeguero",
            created_date="2021-06-16",
            updated_date="2021-10-29",
        )
    }}
