select
deal_id
,portal_id
,is_deleted
,_fivetran_synced
,property_deal_currency_code
,property_days_to_close
,property_deal_attribution
,property_hs_created_by_user_id -- check if this is in fact, if not, move to fact
,property_hs_is_closed
,property_hs_deal_stage_probability
,property_hs_lastmodifieddate
,property_hs_updated_by_user_id -- check if this is in fact, if not, move to fact
,property_hubspot_team_id
,owner_id
,deal_pipeline_id -- check if this is in fact, if not, move to fact
,property_hubspot_owner_assigneddate
,property_dealname
,property_closedate
,property_createdate
,deal_pipeline_stage_id -- check if this is in fact, if not, move to fact
,property_hs_all_owner_ids
,property_hs_is_closed_won
from {{ source('hubspot', 'hubspot_deal') }}