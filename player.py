import sys
import os
import pickle
import pandas as pd
import numpy as np
import statsapi
from unidecode import unidecode

sys.path.append("utils")
from base_class import BaseClass

def convert_innings_pitched(x):
    add_zeros = lambda x : x if x != "" else 0
    return int(float(add_zeros(x))) + (10/3) * (float(add_zeros(x)) - int(float(add_zeros(x))))

class PlayerMap(BaseClass):
    def __init__(self, hitter_stat_names, pitcher_stat_names):
        self.hitter_stat_names = hitter_stat_names
        self.pitcher_stat_names = pitcher_stat_names
        self.hitter_map = {}
        self.pitcher_map = {}

    def add_game_stats_for_hitter(self, player_id, game_name, new_data, opposing_pitcher_id):
        # Process new data
        if "details" not in new_data:
            raise ValueError("Include details column in new data")

        game_stats = self.transform_hitter_stats(new_data, player_id)
        if game_stats is None:
            return

        if player_id in self.hitter_map:
            # Reset counts for certain stats each season
            if game_name[3:7] != self.hitter_map[player_id].stats.index[-1][3:7]:
                for x in ["Home Runs", "Runs Batted In", "At Bats", "Hits", "Runs Scored"]:
                    game_stats[x] = 0
            self.hitter_map[player_id].add_game_stats(game_name, game_stats, opposing_pitcher_id)
        else:
            p = Hitter(player_id, self.hitter_stat_names)
            p.add_game_stats(game_name, game_stats, opposing_pitcher_id)
            self.hitter_map[player_id] = p

    def add_game_stats_for_pitcher(self, player_id, game_name, new_data):
        game_stats = self.transform_pitcher_stats(new_data, player_id)
        if game_stats is None:
            return

        if player_id in self.pitcher_map:
            # Reset counts for certain stats each season
            if game_name[3:7] != self.pitcher_map[player_id].stats.index[-1][3:7]:
                for x in ["Hits", "Runs Scored", "Earned Runs", "Bases on Balls",
                          "Strikeouts", "Home Runs", "Pit", "Str", "Batters Faced"]:
                    game_stats[x] = 0
            self.pitcher_map[player_id].add_game_stats(game_name, game_stats)
        else:
            p = Pitcher(player_id, self.pitcher_stat_names)
            p.add_game_stats(game_name, game_stats)
            self.pitcher_map[player_id] = p

    def transform_pitcher_stats(self, new_data, player_id):
        game_stats = {}
        add_zeros = lambda x : x if x != "" else 0
        for stat_name in self.pitcher_stat_names:
            if stat_name in ["Hits", "Runs Scored", "Earned Runs", "Bases on Balls",
                             "Strikeouts", "Home Runs", "Pit", "Str", "Batters Faced"]:
                try:
                    num = int(add_zeros(new_data[stat_name]))
                except TypeError:
                    self.log(f"TypeError adding stats for {player_id}", error=True)
                    return
                except KeyError:
                    self.log(f"KeyError adding stats for {player_id}", error=True)
                    return
                if player_id in self.pitcher_map:
                    num += self.pitcher_map[player_id].stats.iloc[-1][stat_name]
                game_stats[stat_name] = num
            elif stat_name == "Innings Pitched":
                try:
                    num = convert_innings_pitched(new_data[stat_name])
                except TypeError:
                    self.log(f"TypeError adding stats for {player_id}", error=True)
                    return
                if player_id in self.pitcher_map:
                    num += self.pitcher_map[player_id].stats.iloc[-1][stat_name]
                game_stats[stat_name] = num
            elif stat_name.startswith("Average") or stat_name in ["Games Played", "Innings Pitched Per Game"]:
                pass
            elif stat_name in new_data:
                try:
                    game_stats[stat_name] = float(add_zeros(new_data[stat_name]))
                except TypeError:
                    self.log(f"TypeError adding stats for {player_id}", error=True)
                    return
            else:
                raise ValueError(f"{stat_name} not in stat data")
        return game_stats

    def transform_hitter_stats(self, new_data, player_id):
        game_stats = {}
        add_zeros = lambda x : x if x != "" else 0
        for stat_name in self.hitter_stat_names:
            if stat_name == "Home Runs":
                num_hrs = 0
                if "3·HR" in new_data["details"]:
                    num_hrs = 3
                elif "2·HR" in new_data["details"]:
                    num_hrs = 2
                elif "HR" in new_data["details"]:
                    num_hrs = 1
                if player_id in self.hitter_map:
                    num_hrs += self.hitter_map[player_id].stats.iloc[-1]["Home Runs"]
                game_stats[stat_name] = num_hrs
            elif stat_name in ["Runs Batted In", "At Bats", "Hits", "Runs Scored"]:
                try:
                    num = int(add_zeros(new_data[stat_name]))
                except TypeError:
                    self.log(f"TypeError adding stats for {player_id}", error=True)
                    return
                if player_id in self.hitter_map:
                    num += self.hitter_map[player_id].stats.iloc[-1][stat_name]
                game_stats[stat_name] = num
            elif stat_name.startswith("Average") or stat_name == "At Bats Per Game" or stat_name == "Games Played":
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
        return game_stats

    def get_player_list(self):
        return list(self.hitter_map.keys())

    def get_hitter(self, player_id):
        if player_id in self.hitter_map:
            return self.hitter_map[player_id]
        else:
            self.log(f"Player {player_id} not found")
            return None

    def get_pitcher(self, player_id):
        if player_id in self.pitcher_map:
            return self.pitcher_map[player_id]
        else:
            self.log(f"Player {player_id} not found")
            return None

    def get_player(self, player_id):
        if player_id in self.hitter_map:
            return self.hitter_map[player_id]
        elif player_id in self.pitcher_map:
            return self.pitcher_map[player_id]
        else:
            self.log(f"Player {player_id} not found")
            return None

STATSAPI_KEY_MAP = {
    "Batting Average": "avg",
    "On-Base%": "obp",
    "Slugging %": "slg",
    "Home Runs": "homeRuns",
    "Runs Batted In": "rbi",
    "At Bats": "atBats",
    "Hits": "hits",
    "Runs Scored": "runs",
    "Earned Run Average": "era",
    "Innings Pitched": "inningsPitched",
    "Earned Runs": "earnedRuns",
    "Bases on Balls": "baseOnBalls",
    "Strikeouts": "strikeOuts",
    "Home Runs": "homeRuns",
    "Pit": "numberOfPitches",
    "Str": "strikes",
    "Batters Faced": "battersFaced",
    "Games Played": "gamesPlayed",
}

class Pitcher(BaseClass):
    def __init__(self, player_id, stat_names):
        """Stats must be a dictionary
        """
        self.player_id = player_id
        self.stat_names = list(stat_names)
        self.stats = pd.DataFrame({ x : [0] for x in self.stat_names }, index=["First"])

        # Get player year by year stats
        player_data = statsapi.lookup_player(player_id)
        n = 10
        if len(player_data) == 0:
            # First try removing accents
            player_data = statsapi.lookup_player(unidecode(player_id))

        if len(player_data) == 0:
            # Try to recover player_data by querying n previous years
            c = 0
            current_year = pd.Timestamp.now().year
            while len(player_data) == 0 and c < n:
                player_data = statsapi.lookup_player(player_id, season=current_year-c)
                if len(player_data) == 0:
                    player_data = statsapi.lookup_player(unidecode(player_id), season=current_year-c)
                if len(player_data) > 0:
                    self.log(f"Recovered player data for {player_id}")
                c += 1

        if len(player_data) == 0:
            self.log(f"Player data for {player_id} not found")
            self.yby_data = []
        else:
            player_data = player_data[0]
            self.yby_data = statsapi.player_stat_data(player_data["id"], group="[pitching]", type="yearByYear", sportId=1)["stats"]

    def get_season_stats(self, season):
        input_season = str(season)

        player_pitching_stats = None
        for elem in self.yby_data:
            if elem["season"] == input_season:
                player_pitching_stats = elem["stats"]
                break
        if player_pitching_stats is None:
            # self.log(f"Data for {input_season} not found", error=True)
            return None

        ret = {}
        prefix_string = "Last Season "
        key_map = STATSAPI_KEY_MAP
        innings_pitched = convert_innings_pitched(player_pitching_stats["inningsPitched"])
        for stat in self.stat_names:
            try:
                if stat in key_map.keys():
                    if stat == "Innings Pitched":
                        ret[prefix_string + stat] = innings_pitched
                    else:
                        ret[prefix_string + stat] = float(player_pitching_stats[key_map[stat]])
                elif stat.startswith("Average") and " ".join(stat.split(" ")[1:]) in key_map.keys():
                    average_over = "battersFaced"
                    if int(player_pitching_stats[average_over]) == 0:
                        ret[prefix_string + stat] = 0
                    else:
                        ret[prefix_string + stat] = float(player_pitching_stats[key_map[" ".join(stat.split(" ")[1:])]]) / float(player_pitching_stats[average_over])
                elif stat == "Innings Pitched Per Game":
                    if int(player_pitching_stats["gamesPlayed"]) == 0:
                        ret[prefix_string + stat] = 0
                    else:
                        ret[prefix_string + stat] = float(innings_pitched) / float(player_pitching_stats["gamesPlayed"])
                elif stat == "details" or stat == "Games Played":
                    continue
                else:
                    self.log(f"Stat {stat} not supported", error=True)
            except Exception as e:
                self.log(e, error=True)
                self.log(f"Setting {stat} to 0")
                ret[prefix_string + stat] = 0

        return ret

    def add_game_stats(self, game_id, new_data):
        if game_id in self.stats.index:
            return
        season = game_id[3:7]
        season_mask = [x[3:7] == season for x in list(self.stats.index)]
        games_played = np.sum(season_mask) + 1
        denom = new_data["Batters Faced"]
        for stat in self.stat_names:
            if stat.startswith("Average"):
                if denom == 0:
                    new_data[stat] = 0
                else:
                    new_data[stat] = new_data[" ".join(stat.split()[1:])] / denom
            elif stat == "Innings Pitched Per Game":
                new_data[stat] = new_data["Innings Pitched"] / games_played
            elif stat == "Games Played":
                new_data[stat] = games_played
        self.stats = pd.concat([ self.stats, pd.DataFrame(new_data, index=[game_id]) ])

    def get_stats_before_game(self, game_id, game_date, num_games_threshold=0, include_last_season_data=True):
        i = self.stats.index.get_indexer([game_id])[0]
        stats = self.stats.iloc[i - 1]
        if "Games Played" in stats and int(stats["Games Played"]) < num_games_threshold:
            return None

        if include_last_season_data:
            last_season_stats = self.get_season_stats(game_date.year - 1)
            if last_season_stats is None:
                last_season_stats = {"Last Season " + x : stats[x] for x in stats.keys()}
            stats = dict(stats)
            stats.update(last_season_stats)
            stats = pd.Series(stats)

        return stats

    def get_latest_stats(self, include_last_season_data=True):
        stats = self.stats.iloc[-1]
        if include_last_season_data:
            last_season_stats = self.get_season_stats(pd.Timestamp.now().year - 1)
            if last_season_stats is None:
                last_season_stats = {"Last Season " + x : stats[x] for x in stats.keys()}
            stats = dict(stats)
            stats.update(last_season_stats)
            stats = pd.Series(stats)
        return stats


class Hitter(BaseClass):
    def __init__(self, player_id, stat_names):
        """Stats must be a dictionary
        """
        self.player_id = player_id
        self.stat_names = list(stat_names)
        self.stats = pd.DataFrame({ x : [0] for x in self.stat_names }, index=["First"])
        self.game_id_to_pitcher_id_dict = {}

        # Get player year by year stats
        player_data = statsapi.lookup_player(player_id)
        n = 10
        if len(player_data) == 0:
            # First try removing accents
            player_data = statsapi.lookup_player(unidecode(player_id))

        if len(player_data) == 0:
            # Try to recover player_data by querying n previous years
            c = 0
            current_year = pd.Timestamp.now().year
            while len(player_data) == 0 and c < n:
                player_data = statsapi.lookup_player(player_id, season=current_year-c)
                if len(player_data) == 0:
                    player_data = statsapi.lookup_player(unidecode(player_id), season=current_year-c)
                if len(player_data) > 0:
                    self.log(f"Recovered player data for {player_id}")
                c += 1

        if len(player_data) == 0:
            self.log(f"Player data for {player_id} not found")
            self.yby_data = []
        else:
            player_data = player_data[0]
            self.yby_data = statsapi.player_stat_data(player_data["id"], group="[hitting]", type="yearByYear", sportId=1)["stats"]

    def get_season_stats(self, season):
        input_season = str(season)

        player_hitting_stats = None
        for elem in self.yby_data:
            if elem["season"] == input_season:
                player_hitting_stats = elem["stats"]
                break
        if player_hitting_stats is None:
            # self.log(f"Data for {input_season} not found", error=True)
            return None

        ret = {}
        prefix_string = "Last Season "
        key_map = STATSAPI_KEY_MAP
        for stat in self.stat_names:
            if stat in key_map.keys():
                ret[prefix_string + stat] = float(player_hitting_stats[key_map[stat]])
            elif stat.startswith("Average") and " ".join(stat.split(" ")[1:]) in key_map.keys():
                average_over = "gamesPlayed"
                if int(player_hitting_stats[average_over]) == 0:
                    ret[prefix_string + stat] = 0
                else:
                    ret[prefix_string + stat] = float(player_hitting_stats[key_map[" ".join(stat.split(" ")[1:])]]) / float(player_hitting_stats[average_over])
            elif stat == "At Bats Per Game":
                if int(player_hitting_stats["gamesPlayed"]) == 0:
                    ret[prefix_string + stat] = 0
                else:
                    ret[prefix_string + stat] = float(player_hitting_stats["atBats"]) / float(player_hitting_stats["gamesPlayed"])
            elif stat == "details" or stat == "Games Played":
                continue
            else:
                self.log(f"Stat {stat} not supported", error=True)

        return ret

    def add_game_stats(self, game_id, new_data, opposing_pitcher_id):
        if game_id in self.stats.index:
            return
        season = game_id[3:7]
        season_mask = [x[3:7] == season for x in list(self.stats.index)]
        games_played = np.sum(season_mask) + 1
        for stat in self.stat_names:
            if stat.startswith("Average"):
                if games_played == 0:
                    new_data[stat] = 0
                else:
                    new_data[stat] = new_data[" ".join(stat.split()[1:])] / games_played
            elif stat == "At Bats Per Game":
                new_data[stat] = new_data["At Bats"] / games_played
            elif stat == "Games Played":
                new_data[stat] = games_played
        self.stats = pd.concat([ self.stats, pd.DataFrame(new_data, index=[game_id]) ])
        self.game_id_to_pitcher_id_dict[game_id] = opposing_pitcher_id

    def get_pitcher_id_for_game(self, game_id):
        if game_id not in self.game_id_to_pitcher_id_dict:
            self.log(f"{game_id} not found for {self.player_id}, skipping", error=True)
            return None
        return self.game_id_to_pitcher_id_dict[game_id]

    def get_stats_before_game(self, game_id, game_date, num_games_threshold=0, include_last_season_data=True):
        u, c = np.unique(self.stats.index, return_counts=True)
        dups = u[c > 1]
        if len(dups) > 0:
            dup = dups[0]
            i = 0
            print(self.player_id, game_id)
            for i, x in enumerate(self.stats.index):
                if x == dup:
                    print(self.stats.iloc[i])
        i = self.stats.index.get_indexer([game_id])[0]
        hitting_stats = self.stats.iloc[i - 1]
        if "Games Played" in hitting_stats and int(hitting_stats["Games Played"]) < num_games_threshold:
            return None

        if include_last_season_data:
            last_season_stats = self.get_season_stats(game_date.year - 1)
            if last_season_stats is None:
                last_season_stats = {"Last Season " + x : hitting_stats[x] for x in hitting_stats.keys()}
            hitting_stats = dict(hitting_stats)
            hitting_stats.update(last_season_stats)
            hitting_stats = pd.Series(hitting_stats)

        return hitting_stats

    def did_hit_home_run(self, game_id):
        if game_id == "First":
            return False
        if "details" not in self.stat_names:
            raise BaseException("\"details\" not in stat_names. Cannot get HR data")
        if self.stats.index.get_indexer([game_id])[0] == -1:
            return None
        return "HR" in self.stats.iloc[self.stats.index.get_indexer([game_id])[0]]["details"]

    def get_latest_stats(self, include_last_season_data=True):
        hitting_stats = self.stats.iloc[-1]
        if include_last_season_data:
            last_season_stats = self.get_season_stats(pd.Timestamp.now().year - 1)
            if last_season_stats is None:
                last_season_stats = {"Last Season " + x : hitting_stats[x] for x in hitting_stats.keys()}
            hitting_stats = dict(hitting_stats)
            hitting_stats.update(last_season_stats)
            hitting_stats = pd.Series(hitting_stats)
        return hitting_stats

