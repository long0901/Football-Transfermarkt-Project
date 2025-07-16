with cte1 as (
    select 
        player_id,
        player_name,
        extract(year from date) as season,
        count(1) as games_played,
        sum(yellow_cards) as total_yellow_cards,
        sum(red_cards) as total_red_cards,
        sum(goals) as total_goals,
        sum(assists) as total_assists,
        sum(minutes_played) as total_minutes_played,
        (sum(goals)/sum(minutes_played))*90 as goals_per_90_min,
        (sum(assists)/sum(minutes_played))*90 as assists_per_90_min
    from 
        {{ ref('stg_appearances') }}
    group by 
        player_id, player_name, season
    order by 
        player_id, player_name, season
)
select * from cte1
