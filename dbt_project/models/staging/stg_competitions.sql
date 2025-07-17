select 
    *
from 
    {{ source('staging', 'competitions') }}
