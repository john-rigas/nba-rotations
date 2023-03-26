from Data.Config import stats_calls, drop_table
from nba_api.live.nba.endpoints.playbyplay import PlayByPlay
from SQL.Connections import run_sql
import json
import time

def get_postgres_type(pandas_type):
    if pandas_type == 'float64':
        return 'numeric'
    elif pandas_type == 'int64':
        return 'int'
    else:
        return 'varchar'



class StatsLoader:
    def __init__(self, endpoint, parameters, id_column, table, stats_calls=None, additional_unique_columns=[]):
        self.endpoint = endpoint
        self.parameters = parameters
        self.id_column = id_column
        self.stats_calls = stats_calls
        self.table = table
        self.stats_obj = None
        self.additional_unique_columns = additional_unique_columns
        self.dfs = []

    def run(self):
        print (f'Running - {self.table} - {self.id_column} - {self.parameters}')

        self.stats_obj = self.endpoint(**self.parameters)
        self.dfs = self.stats_obj.get_data_frames()
        names = [r['name'] for r in self.stats_obj.get_dict().get('resultSets', [])]
       
        sql = '''
        insert into nba_api_calls (table_name, parameters, unique_id)
        values (%(table_name)s, %(parameters)s, %(unique_id)s)
        returning id;
        '''
        result = run_sql(query=sql, params={
            'table_name': self.table,
            'parameters': json.dumps(self.parameters),
            'unique_id': self.id_column
        })
        api_call_id = result[0]['id']

        for idx, df in enumerate(self.dfs):
            table_name = f'{self.table}_{names[idx]}' if names else self.table
            df['NBA_API_CALL_ID'] = api_call_id
            if self.id_column not in df.columns and self.id_column.lower() in self.parameters.keys():
                df[self.id_column] = self.parameters[self.id_column.lower()]
            if self.id_column not in df.columns:
                continue
            columns_string = ' '.join(f'{col_name} {get_postgres_type(str(data_type))},' for data_type, col_name in zip(df.dtypes, df.columns))

            a = [a for a in self.additional_unique_columns if a in df.columns]
            unique_string = ','.join([self.id_column] + a)

            if drop_table:
                sql = f'drop table if exists {table_name};'
                run_sql(query=sql)

            sql = f'''
            create table if not exists {table_name} (
                {columns_string}
                create_date timestamp default current_timestamp,
                update_date timestamp default current_timestamp,
                unique({unique_string})
            );
            '''
            run_sql(query=sql)

            column_string = f"({', '.join(df.columns)})"
            value_row_string = f"({', '.join(['%s']*len(df.columns))})"
            set_string = f"{', '.join(f'{c} = excluded.{c}' for c in df.columns)}"

            binds = []
            num_rows = 0
            for _, row in df.iterrows():
                num_rows += 1
                for column in df.columns:
                    binds.append(row[column])

                if len(binds) > 50000:
                    value_string = f"{', '.join([value_row_string]*num_rows)}"
                    sql = f'''
                    INSERT INTO {table_name} {column_string}
                    VALUES {value_string}
                    ON CONFLICT ({unique_string})
                    DO UPDATE SET {set_string};
                    '''
                    run_sql(query=sql, params=binds)

                    num_rows = 0
                    binds = []

            if binds:
                value_string = f"{', '.join([value_row_string]*num_rows)}"
                sql = f'''
                INSERT INTO {table_name} {column_string}
                VALUES {value_string}
                ON CONFLICT ({unique_string})
                DO UPDATE SET {set_string};
                '''
                run_sql(query=sql, params=binds)

            if self.stats_calls:
                for sc in self.stats_calls:
                    for _, row in df.iterrows():
                        time.sleep(2)
                        if sc.get('injected_parameters'):
                            sc['parameters'] = {
                                **sc['parameters'],
                                **{k: row[v] for k,v in sc['injected_parameters'].items()}
                            }

                        dl = StatsLoader(**{k: v for k,v in sc.items() if k != 'injected_parameters'})
                        dl.run()


class PlayLoader:
    def __init__(self, game_id):
        self.game_id = game_id

    def run(self):
        play_obj = PlayByPlay(game_id=self.game_id)
        plays = play_obj.get_dict()['game']['actions']
        if plays:
            plays = [
                {
                    'game_id': self.game_id,
                    'action_number': play['actionNumber'],
                    'play_data': json.dumps(play)
                } for play in plays
            ]

            value_row_string = '(%s, %s, %s)'
            value_string = f"{', '.join([value_row_string]*len(plays))}"
            sql = f'''
            insert into nba_play_by_play (game_id, action_number, play_data)
            values {value_string}
            on conflict do nothing;
            '''
            binds = []
            for play in plays:
                binds.append(play['game_id'])
                binds.append(play['action_number'])
                binds.append(play['play_data'])
            run_sql(query=sql, params=binds)

def run():
    for s in stats_calls:
        dl = StatsLoader(**s)
        dl.run()


    # current = datetime.datetime(2022, 10, 1)
    # dates_to_fetch = []
    # while current <= datetime.datetime.now():
    #     dates_to_fetch.append(str(current.date()))
    #     current += datetime.timedelta(days=1)


    # for date_string in dates_to_fetch:
    #     s = {
    #         'endpoint': Scoreboard,
    #         'table': 'scoreboard',
    #         'id_column': 'GAME_ID',
    #         'additional_unique_columns': ['TEAM_ID'],
    #         'parameters': {
    #             'game_date': date_string,
    #         }
    #     }
    #     dl = StatsLoader(**s)
    #     dl.run()


    # sql = '''
    # select distinct (sa.game_id)
    # from scoreboard_available sa
    # join scoreboard_gameheader sg on sa.game_id = sg.game_id and sg.game_status_text = 'Final'
    # left join nba_play_by_play npbp on sa.game_id = npbp.game_id
    # where npbp.game_id is null;
    # '''
    # result = run_sql(query=sql)
    # game_ids = [g['game_id'] for g in result]
    # for game_id in game_ids:
    #     print (f'Running game - {game_id}')
    #     time.sleep(2)
    #     play_obj = PlayLoader(game_id=game_id)
    #     play_obj.run()

