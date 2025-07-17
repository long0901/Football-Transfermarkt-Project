select 
    player_id,
    season,
    CAST(avg(market_value_in_eur) AS INT64) as average_market_value_in_eur,
    CAST(max(market_value_in_eur) AS INT64) as highest_market_value_in_eur,
    CAST(min(market_value_in_eur) AS INT64) as lowest_market_value_in_eur
from
    {{ ref('stg_player_valuations') }}
group by
    player_id, season

