import sys
import os
import pickle
import pandas as pd
import numpy as np

sys.path.append("utils")
from base_class import BaseClass

class PlayerMap(BaseClass):
    def __init__(self, stat_names):
        self.stat_names = stat_names
        self.map = {}

    def add_game_stats_for_player(self, player_id, game_name, new_data):
        # Process new data
        if "details" not in new_data:
            raise ValueError("Include details column in new data")
        add_zeros = lambda x : x if x != "" else 0
        game_stats = {}
        for stat_name in self.stat_names:
            if stat_name == "Home Runs":
                num_hrs = 0
                if "3·HR" in new_data["details"]:
                    num_hrs = 3
                elif "2·HR" in new_data["details"]:
                    num_hrs = 2
                elif "HR" in new_data["details"]:
                    num_hrs = 1
                if player_id in self.map:
                    num_hrs += self.map[player_id].stats.iloc[-1]["Home Runs"]
                game_stats[stat_name] = num_hrs
            elif stat_name == "Runs Batted In" or stat_name == "At Bats":
                try:
                    num = int(add_zeros(new_data[stat_name]))
                except TypeError:
                    self.log(f"TypeError adding stats for {player_id}", error=True)
                    return
                if player_id in self.map:
                    num += self.map[player_id].stats.iloc[-1][stat_name]
                game_stats[stat_name] = num
            elif stat_name.startswith("Average") or stat_name == "At Bats Per Game":
                pass
            elif stat_name == "details":
                game_stats[stat_name] = new_data[stat_name]
            elif stat_name in new_data:
                try:
                    game_stats[stat_name] = float(add_zeros(new_data[stat_name]))
                except TypeError:
                    self.log(f"TypeError adding stats for {player_id}", error=True)
                    return
            else:
                raise ValueError(f"{stat_name} not in stat data")

        if player_id in self.map:
            if game_name[3:7] != self.map[player_id].stats.index[-1][3:7]:
                game_stats["Home Runs"] = 0
                game_stats["Runs Batted In"] = 0
                game_stats["At Bats"] = 0
            self.map[player_id].add_game_stats(game_name, game_stats)
        else:
            p = Player(player_id, self.stat_names)
            p.add_game_stats(game_name, game_stats)
            self.map[player_id] = p

    def get_player_list(self):
        return list(self.map.keys())

    def get_player(self, player_id):
        if player_id not in self.map:
            self.log(f"Player {player_id} not found")
            return None
        return self.map[player_id]

class Player(BaseClass):
    def __init__(self, player_id, stat_names):
        """Stats must be a dictionary
        """
        self.player_id = player_id
        self.stat_names = list(stat_names)
        self.stats = pd.DataFrame({ x : [0] for x in self.stat_names }, index=["First"])

        # TODO: add functionality for pitchers

    def add_game_stats(self, game_id, new_data):
        for stat in self.stat_names:
            if stat.startswith("Average"):
                if new_data["At Bats"] == 0:
                    new_data[stat] = 0
                else:
                    new_data[stat] = new_data[" ".join(stat.split()[1:])] / new_data["At Bats"]
            elif stat == "At Bats Per Game":
                season = game_id[3:7]
                season_mask = [x[3:7] == season for x in list(self.stats.index)]
                new_data[stat] = new_data["At Bats"] / (np.sum(season_mask) + 1)
        self.stats = pd.concat([ self.stats, pd.DataFrame(new_data, index=[game_id]) ])

    def get_stats_before_game(self, game_id, num_games_threshold=20):
        i = self.stats.index.get_indexer([game_id])[0]
        if i - 1 < num_games_threshold:
            return None
        return self.stats.iloc[i - 1]

    def did_hit_home_run(self, game_id):
        if game_id == "First":
            return False
        if "details" not in self.stat_names:
            raise BaseException("\"details\" not in stat_names. Cannot get HR data")
        if self.stats.index.get_indexer([game_id])[0] == -1:
            return None
        return "HR" in self.stats.iloc[self.stats.index.get_indexer([game_id])[0]]["details"]

    def get_latest_stats(self):
        return self.stats.iloc[- 1]

