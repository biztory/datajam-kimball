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
    rm_users.email || planning_calendar.date || 
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

final as (
    select * from planning_clean
)

select * from final