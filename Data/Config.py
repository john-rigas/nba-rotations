from nba_api.stats.endpoints.leaguedashlineups import LeagueDashLineups
from nba_api.stats.endpoints.leaguedashptstats import LeagueDashPtStats
from nba_api.stats.endpoints.playerdashptshots import PlayerDashPtShots
from nba_api.stats.endpoints.playerdashptreb import PlayerDashPtReb
from nba_api.stats.endpoints.playerdashptpass import PlayerDashPtPass
from nba_api.stats.endpoints.playerdashptshotdefend import PlayerDashPtShotDefend
from nba_api.stats.endpoints.leagueleaders import LeagueLeaders



drop_table = False


stats_calls = [
    # {
    #     'endpoint': LeagueDashLineups,
    #     'table': 'league_dash_lineups',
    #     'id_column': 'GROUP_ID',
    #     'parameters': {
    #         'group_quantity': 5        
    #     }
    # },
    # {
    #     'endpoint': LeagueDashLineups,
    #     'table': 'league_dash_lineups',
    #     'id_column': 'GROUP_ID',
    #     'parameters': {
    #         'group_quantity': 4       
    #     }
    # },
    # {
    #     'endpoint': LeagueDashLineups,
    #     'table': 'league_dash_lineups',
    #     'id_column': 'GROUP_ID',
    #     'parameters': {
    #         'group_quantity': 3      
    #     }
    # },
    # {
    #     'endpoint': LeagueDashLineups,
    #     'table': 'league_dash_lineups',
    #     'id_column': 'GROUP_ID',
    #     'parameters': {
    #         'group_quantity': 2      
    #     }
    # },
    # {
    #     'endpoint': LeagueDashPtStats,
    #     'table': 'league_dash_pt_stats_catch_shoot',
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'pt_measure_type': 'CatchShoot',
    #         'player_or_team': 'Player'   
    #     }
    # },
    # {
    #     'endpoint': LeagueDashPtStats,
    #     'table': 'league_dash_pt_stats_drives',
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'pt_measure_type': 'Drives',
    #         'player_or_team': 'Player'      
    #     }
    # },
    # {
    #     'endpoint': LeagueDashPtStats,
    #     'table': 'league_dash_pt_stats_defense',
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'pt_measure_type': 'Defense',
    #         'player_or_team': 'Player'        
    #     }
    # },
    # {
    #     'endpoint': LeagueDashPtStats,
    #     'table': 'league_dash_pt_stats_possessions',
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'pt_measure_type': 'Possessions',
    #         'player_or_team': 'Player'        
    #     }
    # },
    # {
    #     'endpoint': LeagueDashPtStats,
    #     'table': 'league_dash_pt_stats_pull_up_shot',
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'pt_measure_type': 'PullUpShot',
    #         'player_or_team': 'Player'        
    #     }
    # },
    # {
    #     'endpoint': LeagueDashPtStats,
    #     'table': 'league_dash_pt_stats_efficiency',
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'pt_measure_type': 'Efficiency',
    #         'player_or_team': 'Player'        
    #     }
    # },
    # {
    #     'endpoint': LeagueDashPtStats,
    #     'table': 'league_dash_pt_stats_speed_distance',
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'pt_measure_type': 'SpeedDistance',
    #         'player_or_team': 'Player'        
    #     }
    # },
    # {
    #     'endpoint': LeagueDashPtStats,
    #     'table': 'league_dash_pt_stats_post_touch',
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'pt_measure_type': 'PostTouch',
    #         'player_or_team': 'Player'        
    #     }
    # },
    # {
    #     'endpoint': LeagueDashPtStats,
    #     'table': 'league_dash_pt_stats_paint_touch',
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'pt_measure_type': 'PaintTouch',
    #         'player_or_team': 'Player'        
    #     }
    # },


    {
        'endpoint': LeagueLeaders,
        'table': 'league_leaders_pts',
        'id_column': 'PLAYER_ID',
        'parameters': {
            'per_mode48': 'Totals',
            'stat_category_abbreviation': 'PTS'
        },
        'stats_calls': [
            # {
            #     'endpoint': PlayerDashPtShots,
            #     'table': 'player_dash_pt_shots',
            #     'additional_unique_columns': ['SHOT_TYPE', 'SHOT_CLOCK_RANGE', 'DRIBBLE_RANGE', 'CLOSE_DEF_DIST_RANGE', 'TOUCH_TIME_RANGE'],
            #     'id_column': 'PLAYER_ID',
            #     'injected_parameters': {
            #         'player_id': 'PLAYER_ID'        
            #     },
            #     'parameters': {
            #         'team_id': '0'
            #     }
            # },
            # {
            #     'endpoint': PlayerDashPtReb,
            #     'table': 'player_dash_pt_reb',
            #     'additional_unique_columns': ['SHOT_TYPE_RANGE', 'REB_NUM_CONTESTING_RANGE', 'SHOT_DIST_RANGE', 'REB_DIST_RANGE'],
            #     'id_column': 'PLAYER_ID',
            #     'injected_parameters': {
            #         'player_id': 'PLAYER_ID'        
            #     },
            #    'parameters': {
            #         'team_id': '0'
            #     }
            # },
            # {
            #     'endpoint': PlayerDashPtPass,
            #     'table': 'player_dash_pt_pass',
            #     'id_column': 'PLAYER_ID',
            #     'additional_unique_columns': ['PASS_TEAMMATE_PLAYER_ID', 'PASS_TYPE'],
            #     'injected_parameters': {
            #         'player_id': 'PLAYER_ID'        
            #     },
            #    'parameters': {
            #         'team_id': '0'
            #     }
            # },
            {
                'endpoint': PlayerDashPtShotDefend,
                'table': 'player_dash_pt_shot_defend',
                'additional_unique_columns': ['DEFENSE_CATEGORY'],
                'id_column': 'PLAYER_ID',
                'injected_parameters': {
                    'player_id': 'PLAYER_ID'        
                },
               'parameters': {
                    'team_id': '0'
                }
            },
        ]
    },


    # {
    #     'endpoint': PlayerDashPtReb,
    #     'table': 'player_dash_pt_reb',
    #     'additional_unique_columns': ['SHOT_TYPE_RANGE', 'REB_NUM_CONTESTING_RANGE', 'SHOT_DIST_RANGE', 'REB_DIST_RANGE'],
    #     'id_column': 'PLAYER_ID',
    #     'parameters': {
    #         'team_id': '0',
    #         'player_id': '1630256'
    #     }
    # },

]
