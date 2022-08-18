{{ config(enabled=var('api_source') == 'google_ads' and var('ad_reporting__google_ads_enabled')) }}

with stats as (

    select *
    from {{ var('ad_stats') }}

), accounts as (

    select *
    from {{ var('account_history') }}
    where is_most_recent_record = True
    
), campaigns as (

    select *
    from {{ var('campaign_history') }}
    where is_most_recent_record = True
), campaign_stats as (

    -- ad_stats lacks performance max since those are campaign level only
    select *
    from {{ var('campaign_stats') }}

), ad_groups as (

    select *
    from {{ var('ad_group_history') }}
    where is_most_recent_record = True
    
), ads as (

    select *
    from {{ var('ad_history') }}
    where is_most_recent_record = True
    
), fields as (

    select
        stats.date_day,
        accounts.account_name,
        accounts.account_id,
        campaigns.campaign_name,
        campaigns.campaign_id,
        ad_groups.ad_group_name,
        ad_groups.ad_group_id,
        ads.base_url,
        ads.url_host,
        ads.url_path,
        ads.utm_source,
        ads.utm_medium,
        ads.utm_campaign,
        ads.utm_content,
        ads.utm_term,
        sum(stats.spend) as spend,
        sum(stats.clicks) as clicks,
        sum(stats.impressions) as impressions,
        sum(stats.conversions) as conversions,
        sum(stats.conversions_value) as conversions_value

        {% for metric in var('google_ads__ad_stats_passthrough_metrics') %}
        , sum(stats.{{ metric }}) as {{ metric }}
        {% endfor %}

    from stats
    left join ads
        on stats.ad_id = ads.ad_id
    left join ad_groups
        on ads.ad_group_id = ad_groups.ad_group_id
    left join campaigns
        on ad_groups.campaign_id = campaigns.campaign_id
    left join accounts
        on campaigns.account_id = accounts.account_id
    {{ dbt_utils.group_by(15) }}

    union

    select
        campaign_stats.date_day,
        accounts.account_name as account_name,
        campaign_stats.account_id,
        campaigns.campaign_name,
        campaign_stats.campaign_id,
        null as ad_group_name,
        null as ad_group_id,
        null as base_url,
        null as url_host,
        null as url_path,
        null as utm_source,
        null as utm_medium,
        null as utm_campaign,
        null as utm_content,
        null as utm_term,
        sum(campaign_stats.spend) as spend,
        sum(campaign_stats.clicks) as clicks,
        sum(campaign_stats.impressions) as impressions,
        sum(campaign_stats.conversions) as conversions,
        sum(campaign_stats.conversions_value) as conversions_value
    from campaign_stats
    join campaigns on campaign_stats.campaign_id = campaigns.campaign_id
    left join accounts
        on campaign_stats.account_id = accounts.account_id
    where campaigns.advertising_channel_type = "PERFORMANCE_MAX"
    {{ dbt_utils.group_by(15) }}


)

select *
from fields