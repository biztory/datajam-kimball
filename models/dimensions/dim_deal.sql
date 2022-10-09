with deal as (
    select * from  {{ref('stg_deal')}}
),
final as (
    select
    deal_id
    ,is_deleted
    ,_fivetran_synced
    ,property_deal_currency_code
    ,property_days_to_close
    ,property_deal_attribution
    ,property_hs_is_closed
    ,property_hs_deal_stage_probability
    ,property_hs_lastmodifieddate
    ,property_hubspot_owner_assigneddate
    ,property_dealname
    ,property_closedate
    ,property_createdate
    ,property_hs_is_closed_won
    from deal
)
select * from final