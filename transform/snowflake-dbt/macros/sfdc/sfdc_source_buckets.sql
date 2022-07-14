{%- macro sfdc_source_buckets(lead_source) -%}

case
    when {{ lead_source }} in ('CORE Check-Up', 'Free Registration')
    then 'Core'
    when
        {{ lead_source }}
        in (
            'GitLab Subscription Portal',
            'Gitlab.com',
            'GitLab.com',
            'Trial - Gitlab.com',
            'Trial - GitLab.com'
        )
    then 'GitLab.com'
    when {{ lead_source }} in ('Education', 'OSS')
    then 'Marketing/Community'
    when
        {{ lead_source }}
        in (
            'CE Download',
            'Demo',
            'Drift',
            'Email Request',
            'Email Subscription',
            'Gated Content - General',
            'Gated Content - Report',
            'Gated Content - Video',
            'Gated Content - Whitepaper',
            'Live Event',
            'Newsletter',
            'Request - Contact',
            'Request - Professional Services',
            'Request - Public Sector',
            'Security Newsletter',
            'Trial - Enterprise',
            'Virtual Sponsorship',
            'Web Chat',
            'Web Direct',
            'Web',
            'Webcast'
        )
    then 'Marketing/Inbound'
    when
        {{ lead_source }}
        in ('Advertisement', 'Conference', 'Field Event', 'Owned Event')
    then 'Marketing/Outbound'
    when
        {{ lead_source }}
        in (
            'Clearbit',
            'Datanyze',
            'Leadware',
            'LinkedIn',
            'Prospecting - LeadIQ',
            'Prospecting - General',
            'Prospecting',
            'SDR Generated'
        )
    then 'Prospecting'
    when
        {{ lead_source }}
        in ('Employee Referral', 'External Referral', 'Partner', 'Word of mouth')
    then 'Referral'
    when {{ lead_source }} in ('AE Generated')
    then 'Sales'
    when {{ lead_source }} in ('DiscoverOrg')
    then 'DiscoverOrg'
    else 'Other'
end as net_new_source_categories,
case
    when
        {{ lead_source }}
        in ('CORE Check-Up', 'CE Download', 'CE Usage Ping', 'CE Version Check')
    then 'core'
    when
        {{ lead_source }}
        in (
            'Consultancy Request',
            'Contact Request',
            'Content',
            'Demo',
            'Drift',
            'Education',
            'EE Version Check',
            'Email Request',
            'Email Subscription',
            'Enterprise Trial',
            'Gated Content - eBook',
            'Gated Content - General',
            'Gated Content - Report',
            'Gated Content - Video',
            'Gated Content - Whitepaper',
            'GitLab.com',
            'MovingtoGitLab',
            'Newsletter',
            'OSS',
            'Request - Community',
            'Request - Contact',
            'Request - Professional Services',
            'Request - Public Sector',
            'Security Newsletter',
            'Startup Application',
            'Web',
            'Web Chat',
            'White Paper'
        )
    then 'inbound'
    when
        {{ lead_source }}
        in (
            'AE Generated',
            'Clearbit',
            'Datanyze',
            'DiscoverOrg',
            'Gemnasium',
            'GitLab Hosted',
            'Gitorious',
            'gmail',
            'Leadware',
            'LinkedIn',
            'Live Event',
            'Prospecting',
            'Prospecting - General',
            'Prospecting - LeadIQ',
            'SDR Generated',
            'seamless.ai',
            'Zoominfo'
        )
    then 'outbound'
    when
        {{ lead_source }}
        in (
            'Advertisement',
            'Conference',
            'Executive Roundtable',
            'Field Event',
            'Owned Event',
            'Promotion',
            'Virtual Sponsorship'
        )
    then 'paid demand gen'
    when {{ lead_source }} in ('Purchased List')
    then 'purchased list'
    when
        {{ lead_source }}
        in (
            'Employee Referral',
            'Event Partner',
            'Existing Client',
            'External Referral',
            'Partner',
            'Seminar - Partner',
            'Word of mouth'
        )
    then 'referral'
    when {{ lead_source }} in ('Trial - Enterprise', 'Trial - GitLab.com')
    then 'trial'
    when {{ lead_source }} in ('Webcast', 'Webinar')
    then 'virtual event'
    when {{ lead_source }} in ('GitLab Subscription Portal', 'Web Direct')
    then 'web direct'
    else 'Other'
end as source_buckets,

{%- endmacro -%}
