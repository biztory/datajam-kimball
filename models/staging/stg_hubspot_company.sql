select
id
,portal_id
,is_deleted
,_fivetran_synced
,property_state
,property_name
,property_hs_lastmodifieddate
,property_timezone
,property_city
,property_hubspot_owner_assigneddate
,property_address
,property_address_2
,property_hs_user_ids_of_all_owners
,property_hs_updated_by_user_id
,property_biztory_shortcode
,property_hs_created_by_user_id
,property_hubspot_team_id
,property_industry
,property_zip
,property_web_technologies
,property_createdate
,property_country
,property_hs_all_team_ids
,property_hubspot_owner_id
,property_numberofemployees
,property_hs_parent_company_id
,property_hs_merged_object_ids
,property_hs_object_id
from {{ source('datajam', 'hubspot_company') }}