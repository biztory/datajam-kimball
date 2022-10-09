select
deal_id
,company_id
,_fivetran_synced
from {{ source('hubspot', 'hubspot_deal_company') }}