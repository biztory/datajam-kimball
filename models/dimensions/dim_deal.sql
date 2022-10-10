with deal as (
    select * from  {{ref('stg_deal')}}
),
fact as (
    select * from {{ref('fct_hubspot_li')}}
),
final as (
    select
    deal.deal_id
    ,deal.is_deleted
    ,deal._fivetran_synced
    ,deal.property_deal_currency_code
    ,deal.property_days_to_close
    ,deal.property_deal_attribution
    ,deal.property_hs_is_closed
    ,deal.property_hs_deal_stage_probability
    ,deal.property_hs_lastmodifieddate
    ,deal.property_hubspot_owner_assigneddate
    ,deal.property_dealname
    ,deal.property_closedate
    ,deal.property_createdate
    ,deal.property_hs_is_closed_won
    from deal
)
select * from final