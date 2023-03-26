from fastapi import FastAPI
from SQL.Connections import run_sql
from pydantic import BaseModel
import decimal
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://localhost:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/teams")
def list_teams():
    sql = '''
    select team_id, team
    from nba_teams;
    '''
    result = run_sql(query=sql)
    for team in result:
        team['logo'] = f'https://cdn.nba.com/logos/nba/{team["team_id"]}/primary/L/logo.svg'
    return result

@app.get("/teams/{team_id}")
def get_team(team_id: str):
    sql = '''
    select player_id, player
    from nba_players
    where team_id = %(team_id)s;
    '''
    result = run_sql(query=sql, params={'team_id': team_id})
    for player in result:
        player['image'] = f'https://cdn.nba.com/headshots/nba/latest/260x190/{player["player_id"]}.png'

    sql = '''
    select sa.game_id,
       home.team as home_team,
       home_score.pts as home_pts,
       home.team_id as home_team_id,
       away.team as away_team,
       away_score.pts as away_pts,
       away.team_id as away_team_id,
       case
           when home_score.pts > away_score.pts and home.team_id::int = %(team_id)s then true
           when home_score.pts < away_score.pts and away.team_id::int = %(team_id)s then true
           else false
    end as win
    from scoreboard_available sa
    join scoreboard_gameheader sg on sg.game_id::int = sa.game_id::int
    join nba_teams home on sg.home_team_id::int = home.team_id::int
    join nba_teams away on sg.visitor_team_id::int = away.team_id::int
    join scoreboard_linescore home_score on home.team_id::int = home_score.team_id::int and sg.game_id::int = home_score.game_id::int
    join scoreboard_linescore away_score on away.team_id::int = away_score.team_id::int and sg.game_id::int = away_score.game_id::int
    where sg.home_team_id = %(team_id)s::int or sg.visitor_team_id = %(team_id)s::int;
    '''
    game_result = run_sql(query=sql, params={'team_id': team_id})
    for game in game_result:
        game['away_logo'] = f'https://cdn.nba.com/logos/nba/{game["away_team_id"]}/primary/L/logo.svg'
        game['home_logo'] = f'https://cdn.nba.com/logos/nba/{game["home_team_id"]}/primary/L/logo.svg'

    sql = '''
    select rotation_id, rotation_name, complete
    from nba_rotations
    where team_id = %(team_id)s
    '''
    rotation_result = run_sql(query=sql, params={'team_id': team_id})

    return {'players': result, 'games': game_result, 'rotations': rotation_result, 'team_id': team_id}

@app.get("/teams/{team_id}/stats")
def get_team_stats(team_id: str):
    sql = '''
    select nps.player_id, 
       nps.player,
       llp.*,
       ldpscs.*,
       ldpsd.*,
       ldpsdr.*,
       ldpseff.*,
       ldpspat.*,
       ldpspos.*,
       ldpspt.*,
       ldpspus.*,
       ldpssd.*
    from nba_players_season nps
    left join league_leaders_pts llp on nps.player_id::int = llp.player_id::int
    left join league_dash_pt_stats_catch_shoot ldpscs on nps.player_id::int = ldpscs.player_id::int
    left join league_dash_pt_stats_defense ldpsd on nps.player_id::int = ldpsd.player_id::int
    left join league_dash_pt_stats_drives ldpsdr on ldpsdr.player_id::int = nps.player_id::int
    left join league_dash_pt_stats_efficiency ldpseff on nps.player_id::int = ldpseff.player_id::int
    left join league_dash_pt_stats_paint_touch ldpspat on nps.player_id::int = ldpspat.player_id::int
    left join league_dash_pt_stats_possessions ldpspos on ldpspos.player_id::int = nps.player_id::int
    left join league_dash_pt_stats_post_touch ldpspt on nps.player_id::int = ldpspt.player_id::int
    left join league_dash_pt_stats_pull_up_shot ldpspus on nps.player_id::int = ldpspus.player_id::int
    left join league_dash_pt_stats_speed_distance ldpssd on ldpssd.player_id::int = nps.player_id::int
    where nps.team_id = %(team_id)s;
    '''
    result = run_sql(query=sql, params={'team_id': team_id})
    for row in result:
        for key, value in row.items():
            try:
                if isinstance(value, decimal.Decimal):
                    assert isinstance(value.as_tuple().exponent, int)
            except:
                row[key] = None

    return {'stats': result}

@app.get("/teams/{team_id}/lineup-stats")
def get_lineup_stats(team_id: str):
    sql = '''
    with lineup_counts as (
        select group_id, count(*) as num_players
        from nba_lineups
        group by 1
    ),
    valid_groups as (
    select nl.group_id, lc.num_players, array_agg(nps.player_id) as players
        from nba_lineups nl
        join nba_players_season nps on nl.player_id = nps.player_id and nps.team_id = %(team_id)s
        join lineup_counts lc on lc.group_id = nl.group_id
        group by nl.group_id, lc.num_players
        having lc.num_players = count(*)
    )
    select ldl.*, vg.players
    from league_dash_lineups ldl
    join valid_groups vg on vg.group_id = ldl.group_id
    order by ldl.min desc;
    '''
    result = run_sql(query=sql, params={'team_id': team_id})
    return {'lineups': result}



@app.get("/games/{game_id}/{team_id}")
def get_game(game_id: str, team_id: str):
    sql = '''
    select nsl.player_id::int, np.player
    from nba_starting_lineups nsl
    join nba_players np on np.player_id::int = nsl.player_id::int
    where nsl.game_id = %(game_id)s
    and nsl.team_id = %(team_id)s;
    '''
    starting_lineup_result = run_sql(query=sql, params={'game_id': game_id, 'team_id': team_id})
    for player in starting_lineup_result:
        player['image'] = f'https://cdn.nba.com/headshots/nba/latest/260x190/{player["player_id"]}.png'


    sql = '''
    select 
        jsonb_build_object(
            'player_id', ns.player_id::int,
            'player', np.player
        ) as player,
        ns.quarter, ns.minute, ns.second, ns.centisecond,
        ns.sub_type
    from nba_substitutions ns
    join nba_players np on np.player_id::int = ns.player_id::int
    where ns.game_id = %(game_id)s
    and ns.team_id = %(team_id)s
    order by quarter asc, minute desc, second desc, centisecond desc, action_number asc;
    '''
    substitutions_result = run_sql(query=sql, params={'game_id': game_id, 'team_id': team_id})
    for player in substitutions_result:
        player['player']['image'] = f'https://cdn.nba.com/headshots/nba/latest/260x190/{player["player"]["player_id"]}.png'


    sql = '''
    select sa.game_id,
       home.team as home_team,
       home_score.pts as home_pts,
       home.team_id as home_team_id,
       away.team as away_team,
       away_score.pts as away_pts,
       away.team_id as away_team_id,
       case
           when home_score.pts > away_score.pts and home.team_id::int = %(team_id)s then true
           when home_score.pts < away_score.pts and away.team_id::int = %(team_id)s then true
           else false
    end as win
    from scoreboard_available sa
    join scoreboard_gameheader sg on sg.game_id::int = sa.game_id::int
    join nba_teams home on sg.home_team_id::int = home.team_id::int
    join nba_teams away on sg.visitor_team_id::int = away.team_id::int
    join scoreboard_linescore home_score on home.team_id::int = home_score.team_id::int and sg.game_id::int = home_score.game_id::int
    join scoreboard_linescore away_score on away.team_id::int = away_score.team_id::int and sg.game_id::int = away_score.game_id::int
    where sg.game_id::int = %(game_id)s::int;
    '''
    game_results = run_sql(query=sql, params={'game_id': game_id, 'team_id': team_id})

    if game_results:
        roster = []
        player_ids = set()
        all_players = [s['player'] for s in substitutions_result] + [s for s in starting_lineup_result]
        for p in all_players:
            if p['player_id'] not in player_ids:
                roster.append(p)
                player_ids.add(p['player_id'])

    else:
        sql = '''
        select player_id::int, player
        from nba_players
        where team_id = %(team_id)s;
        '''
        roster = run_sql(query=sql, params={'team_id': team_id})
        for player in roster:
            player['image'] = f'https://cdn.nba.com/headshots/nba/latest/260x190/{player["player_id"]}.png'

    return {
        'starting_lineups': starting_lineup_result,
        'substitutions': substitutions_result,
        'game_result': game_results[0] if game_results else None, 
        'roster': roster
    }


class Rotation(BaseModel):
    starting_lineups: list[str]
    substitutions: list[dict]
    rotation_name: str
    team_id: str
    rotation_id: int = None

@app.post("/rotations")
def create_rotation(rotation: Rotation):
    if not rotation.rotation_id:
        complete = len(rotation.starting_lineups) == 5
        sql = '''
        insert into nba_rotations (complete, rotation_name, team_id)
        values (%(complete)s, %(rotation_name)s, %(team_id)s)
        returning rotation_id;
        '''
        rotation.rotation_id = run_sql(query=sql, params={
            'rotation_name': rotation.rotation_name,
            'team_id': rotation.team_id,
            'complete': complete
        })[0]['rotation_id']

    for player_id in rotation.starting_lineups:
        sql = '''
        insert into nba_starting_lineups (game_id, player_id, team_id)
        values (%(rotation_id)s, %(player_id)s, %(team_id)s)
        on conflict (game_id, player_id) do nothing
        '''
        run_sql(query=sql, params={
            'rotation_id': rotation.rotation_id ,
            'team_id': rotation.team_id,
            'player_id': player_id
        })

    sql = '''
    delete from nba_starting_lineups
    where game_id::int = %(rotation_id)s::int
    and not (player_id = ANY(%(player_ids)s));
    '''
    run_sql(query=sql, params={
        'player_ids': [player_id for player_id in rotation.starting_lineups],
        'rotation_id': rotation.rotation_id 
    })

    sql = '''
    delete from nba_substitutions
    where game_id::int = %(rotation_id)s::int;
    '''
    run_sql(query=sql, params={
        'rotation_id': rotation.rotation_id 
    })

    for substitution in rotation.substitutions:
        sql = '''
        insert into nba_substitutions (game_id, action_number, team_id, player_id, quarter, minute, second, centisecond, sub_type)
        values (%(rotation_id)s, %(action_number)s, %(team_id)s, %(player_id)s, %(quarter)s, %(minute)s, %(second)s, %(centisecond)s, %(sub_type)s)
        on conflict (game_id, action_number) do nothing
        returning action_number;
        '''
        run_sql(query=sql, params={
            'rotation_id': rotation.rotation_id ,
            'action_number': None,
            'team_id': rotation.team_id,
            'player_id': substitution['player_id'],
            'quarter': substitution['quarter'],
            'minute': substitution['minute'],
            'second': substitution['second'],
            'centisecond': substitution.get('centisecond', 0),
            'sub_type': substitution['sub_type']
        })

    return rotation.rotation_id 


# uvicorn app:app --reload --port 8152