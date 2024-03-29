{% snapshot netsuite_budget_category_snapshots %}

    {{
        config(
            strategy="check",
            unique_key="budget_category_id",
            check_cols=[
                "date_deleted",
                "is_global",
                "isinactive",
                "name",
            ],
        )
    }}

    select *
    from {{ source("netsuite", "budget_category") }}

{% endsnapshot %}
