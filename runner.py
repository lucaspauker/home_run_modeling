import sys
import os
import glob
import pickle
import pandas as pd
import numpy as np

sys.path.append("utils")
from base_class import BaseClass
from player import PlayerMap
from game import Game

class Runner(BaseClass):
    def __init__(self, stat_names, pitcher_stat_names, data_dir="./data/game_data"):
        self.data_dir = data_dir
        self.stat_names = stat_names
        self.pitcher_stat_names = pitcher_stat_names
        self.player_map = PlayerMap(self.stat_names, self.pitcher_stat_names)

    def get_games(self):
        return sorted([x.split("/")[-1][:-5] for x in glob.glob(os.path.join(self.data_dir, "*"))], key=lambda x : int(x[3:]))

    def get_game(self, game):
        filename = os.path.join(self.data_dir, game + ".json")
        with open(filename, "r") as f:
            game = Game()
            game.load(filename)
        return game

    def build_player_map_for_all_games(self, n=None):
        self.log("Simulating games")

        # Reset player map
        self.player_map = PlayerMap(self.stat_names, self.pitcher_stat_names)

        filenames = sorted(glob.glob(os.path.join(self.data_dir, "*")), key=lambda x : int(x.split("/")[-1][3:-5]))
        if n is not None:
            filenames = filenames[:n]
        for filename in filenames:
            with open(filename, "r") as f:
                game = Game()
                game.load(filename)

            self.log(f"{game.date.strftime('%m/%d/%y')} {game.home_team} vs. {game.away_team}")

            hitter_stats = game.get_hitter_stats_from_raw_data()
            home_hitter_stats = game.get_home_hitter_stats()
            away_hitter_stats = game.get_away_hitter_stats()
            home_pitcher_stats = game.get_home_pitcher_stats()
            away_pitcher_stats = game.get_away_pitcher_stats()
            self.player_map.add_game_stats_for_pitcher(game.get_home_pitcher(), game.id, home_pitcher_stats)
            self.player_map.add_game_stats_for_pitcher(game.get_away_pitcher(), game.id, away_pitcher_stats)
            if hitter_stats is None:
                continue
            else:
                for hitter in home_hitter_stats:
                    self.player_map.add_game_stats_for_hitter(hitter, game.id, home_hitter_stats[hitter], game.get_away_pitcher())
                for hitter in away_hitter_stats:
                    self.player_map.add_game_stats_for_hitter(hitter, game.id, away_hitter_stats[hitter], game.get_home_pitcher())

    def get_player_list(self):
        return self.player_map.get_player_list()

    def get_player(self, player_id):
        return self.player_map.get_player(player_id)

    def get_stats_for_player_before_game(self, player_id, game_id, game_date, hitter_games_threshold=20,
                                         pitcher_games_threshold=1, include_last_season_data=True):
        player = self.player_map.get_hitter(player_id)
        player_stats = player.get_stats_before_game(game_id,
                                                    game_date,
                                                    num_games_threshold=hitter_games_threshold,
                                                    include_last_season_data=include_last_season_data)
        if player_stats is None:
            return None
        pitcher_id = player.get_pitcher_id_for_game(game_id)
        if pitcher_id is None:
            return None
        pitcher = self.player_map.get_pitcher(pitcher_id)
        pitcher_stats = pitcher.get_stats_before_game(game_id,
                                                      game_date,
                                                      num_games_threshold=pitcher_games_threshold,
                                                      include_last_season_data=include_last_season_data)
        if pitcher_stats is None:
            return None
        pitcher_stats = pitcher_stats.rename({ x : "Opposing Pitcher " + x for x in pitcher_stats.index })
        return pd.concat([player_stats, pitcher_stats])

    def get_latest_stats_for_player_and_pitcher(self, player_id, pitcher_id, include_last_season_data=True):
        player = self.player_map.get_hitter(player_id)
        if player is None:
            return None
        player_stats = player.get_latest_stats(include_last_season_data=include_last_season_data)
        if player_stats is None:
            return None
        pitcher = self.player_map.get_pitcher(pitcher_id)
        if pitcher is None:
            return None
        pitcher_stats = pitcher.get_latest_stats(include_last_season_data=include_last_season_data)
        if pitcher_stats is None:
            return None
        pitcher_stats = pitcher_stats.rename({ x : "Opposing Pitcher " + x for x in pitcher_stats.index })
        return pd.concat([player_stats, pitcher_stats])

