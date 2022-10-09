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
    select * from biztory_team bt
    LEFT JOIN hubspot h ON lower(split(bt.email_address, '@')[0]::string) = lower(split(h.email, '@')[0]::string)
    LEFT JOIN rm_users rm on lower(split(bt.email_address, '@')[0]::string) = lower(split(rm.email, '@')[0]::string)
)
select * from final