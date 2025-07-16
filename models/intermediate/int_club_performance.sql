select 
    club_id,
    season,
    count(1) as total_games_played,
    count(case when hosting='Home' then 1 end) as total_home_games_played,
    count(case when hosting='Away' then 1 end) as total_away_games_played,

    sum(is_win) as total_wins,
    sum(case when hosting='Home' then is_win end) as total_home_wins,
    sum(case when hosting='Away' then is_win end) as total_away_wins,

    sum(case when hosting='Home' then is_win end) / count(case when hosting='Home' then 1 end) as home_win_percentage,
    sum(case when hosting='Away' then is_win end) / count(case when hosting='Away' then 1 end) as away_win_percentage,

    sum(own_goals) as total_goals_scored,
    sum(case when hosting='Home' then own_goals end) as total_home_goals_scored,
    sum(case when hosting='Away' then own_goals end) as total_away_goals_scored,

    sum(opponent_goals) as total_goals_conceded,
    sum(case when hosting='Home' then opponent_goals end) as total_home_goals_conceded,
    sum(case when hosting='Away' then opponent_goals end) as total_away_goals_conceded,

    (sum(own_goals) / count(1)) as average_goals_per_match,
    (sum(case when hosting='Home' then own_goals end) / count(case when hosting='Home' then 1 end)) as average_goals_per_home_match,
    (sum(case when hosting='Away' then own_goals end) / count(case when hosting='Away' then 1 end)) as average_goals_per_away_match

from 
    {{ ref('stg_club_games') }} as cg
join
    {{ ref('stg_games') }} as g 
on 
    cg.game_id = g.game_id
group by club_id, season