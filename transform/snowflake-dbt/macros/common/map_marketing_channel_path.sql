{% macro map_marketing_channel_path(channel_path_column) -%}

case
    when
        {{ channel_path_column }}
        in (
            'Other',
            'Direct',
            'Organic Search.Bing',
            'Web Referral',
            'Social.Twitter',
            'Social.Other',
            'Social.LinkedIn',
            'Social.Facebook',
            'Organic Search.Yahoo',
            'Organic Search.Google',
            'Email',
            'Organic Search.Other',
            'Event.Webcast',
            'Event.Workshop',
            'Content.PF Content',
            'Event.Self-Service Virtual Event'
        )
    then 'Inbound Free Channels'
    when
        {{ channel_path_column }}
        in (
            'Event.Virtual Sponsorship',
            'Paid Search.Other',
            'Event.Executive Roundtables',
            'Paid Social.Twitter',
            'Paid Social.Other',
            'Display.Other',
            'Paid Search.AdWords',
            'Paid Search.Bing',
            'Display.Google',
            'Paid Social.Facebook',
            'Paid Social.LinkedIn',
            'Referral.Referral Program',
            'Content.Content Syndication',
            'Event.Owned Event',
            'Other.Direct Mail',
            'Event.Speaking Session',
            'Content.Gated Content',
            'Event.Field Event',
            'Other.Survey',
            'Event.Sponsored Webcast',
            'Swag.Virtual',
            'Swag.Direct Mail',
            'Event.Conference',
            'Event.Vendor Arranged Meetings'
        )
    then 'Inbound Paid'
    when {{ channel_path_column }} in ('IQM.IQM')
    then 'Outbound'
    when {{ channel_path_column }} in ('Trial.Trial')
    then 'Trial'
    else 'Other'
end

{% endmacro %}
