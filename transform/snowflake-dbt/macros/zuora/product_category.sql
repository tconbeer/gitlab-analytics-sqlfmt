{%- macro product_category(product_column, output_column_name="product_category") -%}

case
    when lower({{ product_column }}) like '%gold%'
    then 'SaaS - Ultimate'
    when lower({{ product_column }}) like '%silver%'
    then 'SaaS - Premium'
    when lower({{ product_column }}) like '%ultimate%'
    then 'Self-Managed - Ultimate'
    when lower({{ product_column }}) like '%premium%'
    then 'Self-Managed - Premium'
    when lower({{ product_column }}) like '%bronze%'
    then 'SaaS - Bronze'
    when lower({{ product_column }}) like '%starter%'
    then 'Self-Managed - Starter'
    when lower({{ product_column }}) like 'gitlab enterprise edition%'
    then 'Self-Managed - Starter'
    when {{ product_column }} = 'Pivotal Cloud Foundry Tile for GitLab EE'
    then 'Self-Managed - Starter'
    when lower({{ product_column }}) like 'plus%'
    then 'Plus'
    when lower({{ product_column }}) like 'standard%'
    then 'Standard'
    when lower({{ product_column }}) like 'basic%'
    then 'Basic'
    when {{ product_column }} = 'Trueup'
    then 'Trueup'
    when ltrim(lower({{ product_column }})) like 'githost%'
    then 'GitHost'
    when
        lower({{ product_column }}) like any (
            '%quick start with ha%', '%proserv training per-seat add-on%'
        )
    then 'Support'
    when
        trim({{ product_column }}) in (
            'GitLab Service Package',
            'Implementation Services Quick Start',
            'Implementation Support',
            'Support Package',
            'Admin Training',
            'CI/CD Training',
            'GitLab Project Management Training',
            'GitLab with Git Basics Training',
            'Travel Expenses',
            'Training Workshop',
            'GitLab for Project Managers Training - Remote',
            'GitLab with Git Basics Training - Remote',
            'GitLab for System Administrators Training - Remote',
            'GitLab CI/CD Training - Remote',
            'InnerSourcing Training - Remote for your team',
            'GitLab DevOps Fundamentals Training',
            'Self-Managed Rapid Results Consulting',
            'Gitlab.com Rapid Results Consulting',
            'GitLab Security Essentials Training - Remote Delivery',
            'InnerSourcing Training - At your site',
            'Migration+',
            'One Time Discount',
            'LDAP Integration',
            'Dedicated Implementation Services',
            'Quick Start without HA, less than 500 users',
            'Jenkins Integration',
            'Hourly Consulting',
            'JIRA Integration',
            'Custom PS Education Services'
        )
    then 'Support'
    when lower({{ product_column }}) like 'gitlab geo%'
    then 'SaaS - Other'
    when lower({{ product_column }}) like 'ci runner%'
    then 'SaaS - Other'
    when lower({{ product_column }}) like 'discount%'
    then 'Other'
    when
        trim({{ product_column }}) in (
            '#movingtogitlab', 'Payment Gateway Test', 'EdCast Settlement Revenue'
        )
    then 'Other'
    when
        trim({{ product_column }}) in (
            'File Locking', 'Time Tracking', '1,000 CI Minutes'
        )
    then 'SaaS - Other'
    when trim({{ product_column }}) in ('Gitlab Storage 10GB')
    then 'Storage'
    else 'Not Applicable'
end as {{ output_column_name }}

{%- endmacro -%}
