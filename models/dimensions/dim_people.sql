-- Contains all attributes of people involved in deals
with hubspot as (
    select * from {{ref('stg_owner')}}
),
biztory_team as (
    select * from {{ref('stg_team_members')}}
),
rm_users as (
    select * from {{ref('stg_users')}}
),
final as (
    select    
    bt.email_address as member_email,
    bt.full_name as member_name,
    bt.country as member_country,
    bt.team as member_team,
    bt.role as member_role, 
    bt.reports_to as member_reports_to,
    bt.biztory_branch as member_biztory_branch,
    bt.start_date as member_start_date,
    bt.end_date as member_end_date,
    bt.status as member_status,
    bt.cronos_login as member_cronos_login,
    bt.phone_number as member_phone_number,
    bt.discipline as member_discipline,
    bt.birthday as member_birthday,
    bt.approver as member_approver,
    bt.member_type as member_type,
    rm.LAST_LOGIN_TIME as rm_last_login_time,
    rm.has_login as rm_has_login,
    rm.account_owner as rm_account_owner,
    rm.billability_target as member_target_billable_rate,
    rm.billable as member_is_billable
    from biztory_team bt
    LEFT JOIN hubspot h ON lower(split(bt.email_address, '@')[0]::string) = lower(split(h.email, '@')[0]::string)
    LEFT JOIN rm_users rm on lower(split(bt.email_address, '@')[0]::string) = lower(split(rm.email, '@')[0]::string)
)
select * from final