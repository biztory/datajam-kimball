with company as (
    select
    id
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
    ,property_hs_updated_by_user_id -- replace this with name of last updated
    ,property_biztory_shortcode
    ,property_hs_created_by_user_id  -- replace this with name of last created
    ,property_hubspot_team_id -- replace this with team assignment if exsists?
    ,property_industry
    ,property_zip
    ,property_web_technologies
    ,property_createdate
    ,property_country
    ,property_hs_all_team_ids
    ,property_hubspot_owner_id -- replace this with name of owner
    ,property_numberofemployees
    ,property_hs_parent_company_id -- see if we have a dimension related to this, else remove
    ,property_hs_merged_object_ids
    ,property_hs_object_id  -- double check its not needed as a key in the fact & remove
from {{ref('stg_company')}}
),
final as (
    select * from company
)
select * from final