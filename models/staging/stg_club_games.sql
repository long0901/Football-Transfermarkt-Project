select
*
from
    {{ source('staging', 'club_games') }}