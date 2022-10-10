with cal as (
    SELECT * from {{ref('calendar')}} 
),

team as (
    select * FROM {{ref('stg_team_members')}} 
),

planning_calendar as (
    select
    team.email_address,
    cal.date
    FROM team
        JOIN cal
        ON cal.date >= team.start_date
        AND cal.date <= COALESCE(team.end_date, DATEADD(day, 365, CURRENT_DATE))
    WHERE team.role='Consultant' AND status='Active' AND cal.day_of_week > 0 AND cal.day_of_week < 6 and (DATE_PART(month,cal.date) = 7 or DATE_PART(month,cal.date) = 8)
),

rm_mgr_assignment as (
    select * from {{ref('stg_assignments')}}
),

rm_users as (
    select * from {{ref('stg_users')}}
),

rm_pp as (
    select 
    *
    from {{ref('stg_project_phases')}}
),


planning_clean as (
    SELECT 
	rm_users.email AS consultant_email,
	rm_users.display_name AS consultant,
	planning_calendar.date AS engagement_date,
	rm_pp.name AS planned,
	rm_pp.project_code,
	rm_pp.project_state AS engagement_status,
	SPLIT(rm_pp.project_code, ' | ')[0]::STRING AS hubspot_deal_id,
	SPLIT(rm_pp.project_code, ' | ')[1]::STRING AS hubspot_line_item,
	COALESCE(rm_mgr_assignment.hours_per_day, 8) AS hours,
	rm_mgr_assignment.description AS rm_assignment_description
FROM planning_calendar
    JOIN rm_users ON LOWER(SPLIT(rm_users.email, '@')[0]::string) = LOWER(SPLIT(planning_calendar.email_address, '@')[0]::string) 
    LEFT JOIN  rm_mgr_assignment
        ON planning_calendar.date >= rm_mgr_assignment.starts_at AND planning_calendar.date <= rm_mgr_assignment.ends_at
        AND rm_mgr_assignment.user_id = rm_users.id
    LEFT JOIN rm_pp ON rm_mgr_assignment.assignable_id = rm_pp.id
),

hubspot_deal as (
    select * from {{ref('stg_deal')}}
    --deal_id as planned_deal_id
    --from {{ref('stg_deal')}}
), -- join on deal id

hubspot_deal_company as (
    select * from {{ref('stg_deal_company')}}
    --deal_id,
    --company_id as planned_company_id
    --from {{ref('stg_deal_company')}}
), -- join on deal id

hubspot_li as (
    select
    id,
    deal_id, 
    property_name,
    min(property_price) as bill_rate,
    ((SUM(property_quantity * COALESCE(property_product_hours,1)))/8) AS line_item_num_days
    from {{ref('stg_line_item')}}
    where (property_sub_category = 'Consulting' OR property_sub_category = 'Training')
    group by 1,2,3
),

dim_company as (
    select 
    id
    from {{ref('dim_company')}}
),

dim_deal as (
    select
    deal_id
    from {{ref('dim_deal')}}
),

dim_people as (
    select member_email
    from {{ref('dim_people')}}
),

final as (
    select
    pc.consultant_email, 
    pc.engagement_date,
    pc.hours,
    pc.project_code,
    hli.bill_rate,
    hli.line_item_num_days,
    dd.deal_id as planned_deal_id,
    dc.id as planned_company_id,
    -- below to be split into dimension
    pc.engagement_status,
    pc.planned
    -- add deal owner id
    -- add deal team id
    FROM planning_clean pc
    LEFT JOIN hubspot_li hli on hli.deal_id = pc.hubspot_deal_id
    AND hli.property_name = pc.hubspot_line_item
    -- get our deal id & join to our dimension
    LEFT JOIN hubspot_deal hd on hd.deal_id = pc.hubspot_deal_id
    LEFT JOIN dim_deal dd on dd.deal_id = hd.deal_id
     -- get our company id & join to our dimension
    LEFT JOIN hubspot_deal_company hdc on hdc.deal_id = pc.hubspot_deal_id
    LEFT JOIN dim_company dc on dc.id = hdc.company_id
    -- get our people id & join to our dimension
    LEFT JOIN dim_people dp on lower(split(pc.consultant_email, '@')[0]::string) = lower(split(dp.member_email, '@')[0]::string)
)

select * from final