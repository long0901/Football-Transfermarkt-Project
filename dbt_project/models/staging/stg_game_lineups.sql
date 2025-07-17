select
    *
from
    {{ source('staging', 'game_lineups') }}