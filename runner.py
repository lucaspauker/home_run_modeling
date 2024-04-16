import sys
import os
import glob
import pickle
import pandas as pd
import numpy as np

sys.path.append("utils")
from base_class import BaseClass
from player import PlayerMap

class Runner(BaseClass):
    def __init__(self, stat_names, data_dir="./data/game_data"):
        self.data_dir = data_dir
        self.stat_names = stat_names
        self.player_map = PlayerMap(self.stat_names)

    def get_games(self):
        return sorted([x.split("/")[-1][:-2] for x in glob.glob(os.path.join(self.data_dir, "*"))], key=lambda x : int(x[3:]))

    def get_game(self, game):
        filename = os.path.join(self.data_dir, game + ".p")
        with open(filename, "rb") as f:
            game = pickle.load(f)
        return game

    def build_player_map_for_all_games(self, n=None):
        self.log("Simulating games")

        # Reset player map
        self.player_map = PlayerMap(self.stat_names)

        filenames = sorted(glob.glob(os.path.join(self.data_dir, "*")), key=lambda x : int(x.split("/")[-1][3:-2]))
        if n is not None:
            filenames = filenames[:n]
        for filename in filenames:
            with open(filename, "rb") as f:
                game = pickle.load(f)

            self.log(f"{game.date.strftime('%m/%d/%y')} {game.home_team} vs. {game.away_team}")

            hitter_stats = game.get_hitter_stats_from_raw_data()
            if hitter_stats is None:
                continue
            for hitter in hitter_stats:
                self.player_map.add_game_stats_for_player(hitter, game.id, hitter_stats[hitter])

    def get_player_list(self):
        return self.player_map.get_player_list()

    def get_player(self, player_id):
        return self.player_map.get_player(player_id)

