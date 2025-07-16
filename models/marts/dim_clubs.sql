select
    cp.season,
    cp.club_id,
    club_name,
    total_games_played,
    total_home_games_played,
    total_away_games_played,
    total_wins,
    total_home_wins,
    total_away_wins,
    ROUND(home_win_percentage, 3) as home_win_percentage,
    ROUND(away_win_percentage, 3) as away_win_percentage,
    total_goals_scored,
    total_home_goals_scored,
    total_away_goals_scored,
    total_goals_conceded,
    total_home_goals_conceded,
    total_away_goals_conceded,
    ROUND(average_goals_per_match, 3) as average_goals_per_match,
    ROUND(average_goals_per_home_match, 3) as average_goals_per_home_match,
    ROUND(average_goals_per_away_match, 3) as average_goals_per_away_match,
    CAST(total_transfers_income AS INT64) as total_transfers_income,
    CAST(total_transfers_spending AS INT64) as total_transfers_spending,
    CAST(net_transfer_balance AS INT64) as net_transfer_balance
from
    {{ ref('int_club_performance') }} as cp
join
    {{ ref('int_club_transfers') }} as ct
on 
    cp.club_id = ct.club_id 
and 
    cp.season = ct.season
join
    {{ ref('stg_clubs') }} as c
on
    cp.club_id = c.club_id
order by season desc