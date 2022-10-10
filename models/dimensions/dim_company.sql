with company as (
    select * from {{ref('stg_company')}}
),
final as (
    select 
    company.id
    ,company.is_deleted
    ,company._fivetran_synced
    ,company.property_state
    ,company.property_name
    ,company.property_hs_lastmodifieddate
    ,company.property_timezone
    ,company.property_city
    ,company.property_hubspot_owner_assigneddate
    ,company.property_address
    ,company.property_address_2
    ,company.property_hs_user_ids_of_all_owners
    ,company.property_hs_updated_by_user_id -- replace this with name of last updated
    ,company.property_biztory_shortcode
    ,company.property_hs_created_by_user_id  -- replace this with name of last created
    ,company.property_hubspot_team_id -- replace this with team assignment if exsists?
    ,company.property_industry
    ,company.property_zip
    ,company.property_web_technologies
    ,company.property_createdate
    ,company.property_country
    ,company.property_hs_all_team_ids
    ,company.property_hubspot_owner_id -- replace this with name of owner
    ,company.property_numberofemployees
    ,company.property_hs_parent_company_id -- see if we have a dimension related to this, else remove
    ,company.property_hs_merged_object_ids
    ,company.property_hs_object_id
    from company
)
select * from final