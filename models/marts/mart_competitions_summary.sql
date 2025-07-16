select
    season,
    g.competition_id,
    competition_name,
    country_name,
    count(1) as total_games_played,
    sum(home_club_goals + away_club_goals) as total_goals_scored,
    ROUND(sum(home_club_goals + away_club_goals) / count(1), 3) as average_goals_per_match,
    CAST(avg(attendance) AS INT64) as average_attendance
from
    {{ ref('stg_games') }} as g
join
    {{ ref('stg_competitions') }} as c
on
    g.competition_id = c.competition_id
group by 
    season, g.competition_id, competition_name, country_name
order by 
     season desc, g.competition_id