select 
    prf.season,
    prf.player_id,
    prf.player_name,
    position,
    nationality,
    games_played,
    total_minutes_played,
    total_goals,
    total_assists,
    total_yellow_cards,
    total_red_cards,
    ROUND(goals_per_90_min, 3) as goals_per_90_min,
    ROUND(assists_per_90_min, 3) as assists_per_90_min,
    CAST(starting_percentage AS INT64) as starting_percentage,
    CAST(substituting_percentage  AS INT64) as substitute_percentage,
    CAST(win_percentage_when_starting AS INT64) as win_percentage_when_starting,
    CAST(win_percentage_when_substitute AS INT64) as win_percentage_when_substitute,
    pmv.average_market_value_in_eur,
    pmv.highest_market_value_in_eur,
    pmv.lowest_market_value_in_eur,
    p.image_url
from {{ ref('int_player_performance') }} as prf
join
    {{ ref('int_player_participation') }} as prt
on 
    prf.player_id = prt.player_id 
and 
    prf.season = prt.season
join
    {{ ref('int_player_market_value') }} as pmv 
on
    prf.player_id = pmv.player_id
and
    prf.season = pmv.season
join
    {{ ref('stg_players') }} as p
on
    prf.player_id = p.player_id
order by season desc