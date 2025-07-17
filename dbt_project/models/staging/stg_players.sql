select 
    *
from 
    {{ source('staging', 'players') }}

