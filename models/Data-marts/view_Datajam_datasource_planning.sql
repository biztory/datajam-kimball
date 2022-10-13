with fact as (
    select * from {{ref('fct_hubspot_li')}}
),

dim_company as (
    select * from {{ref('dim_company')}}
),

dim_people as (
    select * from {{ref('dim_people')}}
),

dim_deal as (
    select * from {{ref('dim_deal')}}
),

dim_engagment as (
    select * from {{ref('dim_engagement')}}
),

final as (
    select
    dp.member_name as "Consultant",
    dp.member_biztory_branch as "Consultant Biztory Branch",
    dp.member_country as "Country",
    dp.member_cronos_login as "Consultant Cronos Login",
    dp.member_email as "Consultant Email",
    dp.member_end_date as "Consultant End Date",
    dp.member_start_date as "Consultant Start Date",
    dp.member_type as "Consultant Member Type",
    dp.member_reports_to as "Consultant Reports To",
    dp.member_role as "Consultant Role",
    dp.member_status as "Consultant Status",
    dp.member_team as "Consultant Team",
    fct.engagement_date as "Engagement Date",
    ifnull(fct.engagement_status,'Free') in  as "Engagement Status",
    fct.planned as "Planned",
    dc.property_biztory_shortcode as "Planned Biztory Shortcode",
    COALESCE(dc.property_address,dc.property_address_2,'unknown') as "Planned Company Address",
    COALESCE(dc.property_city,'unknown') as "Planned Company City",
    COALESCE(dc.property_country,'unknown') as "Planned Company Country",
    fct.planned_company_id as "Planned Company Id",
    COALESCE(dc.property_name,'unknown') as "Planned Company Name",
    COALESCE(dc.property_state,'unknown') as "Planned Company State",
    COALESCE(dc.property_zip,'unknown') as "Planned Company Zip",
    fct.planned_deal_id as "Planned Deal Id",
    dd.deal_owner as "Planned Deal Owner",
    fct.project_code as "Project Code",
    --de.request_comments as "Request Comments",
    --de.request_created as "Request Created",
    --de.rm_assignment_description as "Rm Assignment Description",
    fct.bill_rate as "Bill Rate",
    fct.hours as "Hours",
    fct.line_item_num_days as "Line Item Num Days"
    FROM fact fct
    LEFT JOIN dim_people dp on LOWER(SPLIT(dp.member_email, '@')[0]::string) = LOWER(SPLIT(fct.consultant_email, '@')[0]::string)
    LEFT JOIN dim_company dc on fct.planned_company_id = dc.id
    LEFT JOIN dim_deal dd on fct.planned_deal_id = dd.deal_id
)
select * from final
