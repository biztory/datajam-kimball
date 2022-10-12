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
	su.email AS consultant_email,
	su.display_name AS consultant,
	cal.date AS engagement_date,
	sp.name AS planned,
	-- sp.client AS client, -- We'd probably rather pull this from HubSpot
	sp.project_code,
	sp.project_state AS engagement_status,
	SPLIT(sp.project_code, ' | ')[0]::STRING AS hubspot_deal_id,
	SPLIT(sp.project_code, ' | ')[1]::STRING AS hubspot_line_item,
	COALESCE(sa.hours_per_day, 8) AS hours, -- Default to 8
	sa.description AS rm_assignment_description
    FROM planning_calendar cal
    JOIN rm_users su ON LOWER(SPLIT(su.email, '@')[0]::string) = LOWER(SPLIT(cal.email_address, '@')[0]::string) 
    LEFT JOIN  rm_mgr_assignment sa
        ON cal.date >= sa.starts_at AND cal.date <= sa.ends_at
        AND sa.user_id = su.id
    LEFT JOIN rm_pp sp ON sa.assignable_id = sp.id
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
    select deal_id,
    property_name,
    ((SUM(property_quantity * COALESCE(property_product_hours,1)))/8) AS line_item_num_days,
    min(property_price) as bill_rate 
    from {{ref('stg_line_item')}}
    where (property_sub_category = 'Consulting' OR property_sub_category = 'Training')
    group by 1,2
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
responses as (
    select hub_spot_line_item,
    TO_VARCHAR(CONVERT_TIMEZONE('Europe/Warsaw', 'UTC',TO_TIMESTAMP(created,'DD/MM/YYYY hh24:mi')), 'YYYY-MM-DDThh:mi:ssZ') as created,
    comments_add_information
    from {{ref('stg_request_form_responses')}}
),

latest_responses as (
    select rq.hub_spot_line_item, rq.created as request_created, rq.comments_add_information as request_comments
    from responses rq
    join ( -- determine most recent request in case there is more than one request per line item (unlikely but possible)
        select hub_spot_line_item, max(created) as created_max
        from responses
        group by 1
        limit 1
        ) rq_most_recent
        where rq.hub_spot_line_item = rq_most_recent.hub_spot_line_item and rq.created = rq_most_recent.created_max
),
orrf as (
    select hub_spot_line_item,
    TO_VARCHAR(CONVERT_TIMEZONE('Europe/Warsaw', 'UTC',TO_TIMESTAMP(created,'DD/MM/YYYY hh24:mi')), 'YYYY-MM-DDThh:mi:ssZ') as created,
    comments_add_information
    from {{ref('stg_request_form_responses')}}
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
    pc.planned,
    pc.rm_assignment_description as "Rm Assignment Description",
    rq.request_created as "Request Created",
    rq.request_comments as "Request Comments"
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
    LEFT JOIN (
        SELECT rq.hub_spot_line_item, rq.created AS request_created, rq.comments_add_information AS request_comments
        FROM orrf rq
            JOIN ( -- Determine most recent request in case there is more than one request per line item (unlikely but possible)
                SELECT hub_spot_line_item, MAX(created) AS created_max
                FROM orrf
                GROUP BY 1
                LIMIT 1
            ) rq_most_recent
        WHERE rq.hub_spot_line_item = rq_most_recent.hub_spot_line_item AND rq.created = rq_most_recent.created_max
    ) rq ON pc.project_code = rq.hub_spot_line_item
)

select * from final