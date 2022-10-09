select
owner_id
,portal_id
,type
,first_name
,last_name
,email
,created_at
,updated_at
,_fivetran_synced
,active_user_id
,is_active
,user_id_including_inactive
from {{ source('hubspot', 'hubspot_owner') }}