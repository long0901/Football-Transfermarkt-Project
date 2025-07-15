select 
    *
from
    {{ source('staging', 'player_valuations') }}