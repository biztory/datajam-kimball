select
DATE
,DAY
,DAY_OF_WEEK
,DAY_OF_YEAR
,MONTH
,MONTH_NAME
,WEEK_OF_YEAR
,YEAR
from {{ source('datajam', 'calendar') }}