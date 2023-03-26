create table if not exists nba_api_calls (
    id serial primary key,
    endpoint varchar not null,
    parameters jsonb default '{}',
    unique_id varchar,
    create_date timestamp
);


create table if not exists nba_play_by_play (
    game_id varchar,
    action_number integer,
    play_data jsonb,
    unique(game_id, action_number)
);

create table if not exists nba_substitutions (
    game_id varchar,
    action_number integer,
    quarter integer,
    minute integer,
    second integer,
    centisecond integer,
    sub_type varchar,
    player_id varchar,
    team_id varchar,
    unique(game_id, action_number)
);


insert into nba_substitutions (
    game_id,
    action_number,
    quarter,
    minute,
    second,
    centisecond,
    sub_type,
    player_id,
    team_id
)
select
    game_id,
    action_number,
    (play_data ->> 'period')::int as quarter,
    substr(play_data ->> 'clock', 3, 2)::int as minute,
    substr(play_data ->> 'clock', 6, 2)::int as second,
    substr(play_data ->> 'clock', 9, 2)::int as centisecond,
    play_data ->> 'subType' as sub_type,
    play_data -> 'personIdsFilter' ->> 0 as player_id,
    play_data ->> 'teamId' as team_id
from nba_play_by_play
where play_data ->> 'actionType' = 'substitution'
on conflict (game_id, action_number)
do nothing;


create table if not exists nba_starting_lineups (
    game_id varchar,
    player_id integer,
    team_id varchar,
    unique(game_id, player_id)
);

with ranked_subs as (
    select game_id, player_id, sub_type, team_id,
     rank() over (partition by game_id, player_id order by quarter asc, minute desc, second desc, centisecond desc, action_number asc) as rank
    from nba_substitutions
)
insert into nba_starting_lineups(game_id, player_id, team_id)
select game_id, player_id::int, team_id
from ranked_subs
where rank = 1
and sub_type = 'out'
on conflict (game_id, player_id) do nothing;


create table if not exists nba_teams (
    team_id varchar,
    team varchar,
    unique(team_id)
);

insert into nba_teams (team_id, team)
select distinct team_id, team
from league_leaders_pts
on conflict (team_id) do nothing;


create table if not exists nba_players (
    player_id varchar,
    team_id varchar,
    player varchar,
    unique(player_id)
);

insert into nba_players (player_id, team_id, player)
select distinct player_id, team_id, player
from league_leaders_pts
on conflict do nothing;


create table if not exists nba_rotations (
    rotation_id serial primary key,
    complete boolean,
    rotation_name varchar,
    team_id varchar
);

create table if not exists nba_players_season (
    player_id varchar,
    team_id varchar,
    player varchar,
    unique(player_id, team_id)
);

insert into nba_players_season (player_id, team_id, player)
select distinct ns.player_id, ns.team_id, np.player
from nba_substitutions ns 
join nba_players np on np.player_id = ns.player_id
union
select player_id, team_id, player
from nba_players
on conflict (player_id, team_id) do nothing;

create table if not exists nba_lineups (
    group_id varchar,
    player_id varchar,
    unique(group_id, player_id)
);

insert into nba_lineups (group_id, player_id)
select distinct ldl.group_id, np.player_id
from league_dash_lineups ldl
join nba_players np on  ldl.group_id ilike '%' || np.player_id || '%'
on conflict (group_id, player_id) do nothing;