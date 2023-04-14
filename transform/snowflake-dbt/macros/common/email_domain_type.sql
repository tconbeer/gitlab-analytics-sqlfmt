{% macro email_domain_type(email_domain, lead_source) %}

    {%- set personal_email_domains_partial_match = get_personal_email_domain_list(
        "partial_match"
    ) -%}
    {%- set personal_email_domains_full_match = get_personal_email_domain_list(
        "full_match"
    ) -%}

    case
        when
            {{ lead_source }}
            in ('DiscoverOrg', 'Zoominfo', 'Purchased List', 'GitLab.com')
        then 'Bulk load or list purchase or spam impacted'
        when trim({{ email_domain }}) is null
        then 'Missing email domain'

        when
            {{ email_domain }} like any (
                {%- for personal_email_domain in personal_email_domains_partial_match -%}
                    '%{{personal_email_domain}}%' {%- if not loop.last -%}, {% endif %}
                {% endfor %}
            )

            or {{ email_domain }} in (
                {%- for personal_email_domain in personal_email_domains_full_match -%}
                    '{{personal_email_domain}}' {%- if not loop.last -%}, {% endif %}
                {% endfor %}
            )

        then 'Personal email domain'
        else 'Business email domain'
    end

{% endmacro %}
