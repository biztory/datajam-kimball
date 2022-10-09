select
deal_id
,company_id
,_fivetran_synced
from {{ source('datajam', 'hubspot_deal_company') }}