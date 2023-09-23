{{
    config(
        materialized='incremental'
    )
}}

WITH random_number AS (SELECT LPAD(TRUNC(random() * 10000000000000)::text, 13, '0')::text AS random_id)
SELECT COALESCE(client_code || random_id,'TEST')  as export_id,
client_code || random_id || '_'|| row_number() OVER () AS export_row_id,
COALESCE(LOWER("Mapped Client"),'TEST') as client,
-1 as reporting_year,
-1 as reporting_month,
'empty' as reporting_date,
"Market" as market,
"Mapped Agency" as data_provider,
'Direct Buys' as data_provider_source_type,
COALESCE("Mapped Agency",'test') as partner,
'Agency' as partner_type,
'Direct Buys' as source_type,
'Direct Buys' as source,
'month' as export_type,
currency as currency,
nextval('direct_buys.direct_buys_month_id_seq'::regclass) as id,
"Campaign Name" as campaign,
NULL as campaign_detail, -- not in recent template
"Campaign Objective" as objective, -- doesn't exist
NULL as objective_cat, -- not in recent template
"KPI" as kpi,
"KPI Target" as kpi_target,
"Mapped Buy metric" as buy_metric,
 "Start Date" as start_date,
 "Start Date"::date as start_date_cat,
"End Date" as end_date,
"End Date"::date as end_date_cat,
"Mapped Supplier" as sales_house_supplier,
"Mapped Domain" as domain,
"Ad Format" as ad_format_creative_length,
"Mapped Buy Type" as buy_type,
NULL as buying_units, -- not in recent template
1 as planned_units, -- not in recent template
"Spend" as spend,
NULL::numeric as gross_cost,  -- not in recent template
currency_converter as currency_converter,
"Spend"*currency_converter spend_client_currency,
NULL::numeric gross_cost_client_currency, -- not in recent template, derived from gross_cost
"Delivered Impressions" as impressions,
"Delivered Clicks" as clicks,
"Delivered Video Views" as video_views,
NULL::numeric as page_views, -- not in recent template
"Delivered Social Engagements" as engagements,
"Delivered Actions" as actions,
"Video Starts" as video_starts,
"Video Completions" as video_watched_100_percent,
NULL::numeric as ad_verification_implemented, -- not in recent template
'Digital Decisions' as ad_verification_partner,-- doesn't exist
NULL::numeric as total_tracked_impressions, -- not in recent template
NULL::numeric as total_measured_impressions,-- not in recent template
"Measured Impressions for Viewability" as measured_impressions_for_viewability, -- doesn't exist
1 as viewable_impressions, -- unclear
NULL::numeric as measured_impressions_for_ivt_detection, -- not in recent template
NULL::numeric as invalid_traffic_impressions, -- not in recent template
NULL::numeric as measured_impressions_for_brand_safety, -- not in recent template
NULL::numeric as brand_unsafe_impressions, -- not in recent template
NULL::varchar as month, -- not in recent template
NULL::date as month_cat, -- not in recent template, derived from gross_cost
"Mapped Brand" as brand,
'Uncategorized' as brand_cat,
NULL::varchar as business_unit, -- not in recent template
'Uncategorized' as business_unit_cat,
FALSE as mission_control, --NOT PRESENT
NULL::varchar as domain_cat_old, --NOT PRESENT
CURRENT_DATE as import_date,
NULL as product_category,-- not in recent template
NULL as ad_verification_y_n,-- not in recent template
'test' as ad_verification_campaign_name,
'test' as notes,
"Mapped Channel" as format,
NULL as format_cat,
NULL as domain_cat,
FALSE as exposhield
FROM {{ source('peerdb_raw', 'raw_union_pool') }}
r LEFT JOIN {{ source('direct_buys', 'client_code_mapping') }} cm ON r."Mapped Client"=cm.client_name CROSS JOIN random_number
LEFT JOIN {{ source('direct_buys', 'currency_mapping') }} currmap ON r."Market"=currmap.market -- TALK TO BOB about this