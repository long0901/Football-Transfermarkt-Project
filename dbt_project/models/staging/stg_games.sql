select 
    *
from 
    {{ source('staging', 'games') }}