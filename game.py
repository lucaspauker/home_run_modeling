import sys
import os
import json
import pickle
import pandas as pd
import numpy as np

sys.path.append("utils")
from base_class import BaseClass

class Game(BaseClass):
    def __init__(
                self,
                id_ = None,
                time = None,
                date = None,
                venue = None,
                home_team = None,
                away_team = None,
                home_team_batting_df = None,
                away_team_batting_df = None,
                home_team_pitching_df = None,
                away_team_pitching_df = None,
            ):
        """All data passed to the game object is data that either occurred during
        the game or calculated after (i.e. batting average).
        """
        self.id = id_
        self.time = time
        self.date = date
        self.venue = venue
        self.home_team = home_team
        self.away_team = away_team
        self.home_team_batting_df = home_team_batting_df
        self.away_team_batting_df = away_team_batting_df
        self.home_team_pitching_df = home_team_pitching_df
        self.away_team_pitching_df = away_team_pitching_df

    def get_game_data(self):
        return {
                "id": self.id,
                "time": self.time,
                "date": pd.Timestamp(self.date).strftime("%Y-%m-%d"),
                "venue": self.venue,
                "home_team": self.home_team,
                "away_team": self.away_team,
                "home_team_batting_df": self.home_team_batting_df.to_dict(),
                "away_team_batting_df": self.away_team_batting_df.to_dict(),
                "home_team_pitching_df": self.home_team_pitching_df.to_dict(),
                "away_team_pitching_df": self.away_team_pitching_df.to_dict(),
            }

    def load_game_data(self, d):
        self.id = d["id"]
        self.time = d["time"]
        self.date = pd.Timestamp(d["date"])
        self.venue = d["venue"]
        self.home_team = d["home_team"]
        self.away_team = d["away_team"]
        self.home_team_batting_df = pd.DataFrame(d["home_team_batting_df"])
        self.away_team_batting_df = pd.DataFrame(d["away_team_batting_df"])
        self.home_team_pitching_df = pd.DataFrame(d["home_team_pitching_df"])
        self.away_team_pitching_df = pd.DataFrame(d["away_team_pitching_df"])

    def save(self, data_dir="./data/game_data"):
        filename = os.path.join(data_dir, self.id + ".json")
        self.log(f"Saving game {self.id} to {filename}")
        with open(filename, "w") as f:
            json.dump(self.get_game_data(), f)

    def load(self, filename="./data/game_data"):
        with open(filename, "r") as f:
            self.load_game_data(json.load(f))
        # self.log(f"Data recovered for {self.id} from {filename}")

    def get_hitter_stats_from_raw_data(self):
        """
        Returns stats for hitters based on what has happened in the game + any games before it.
        stats_before_game is df with index of player names/IDs.
        """
        stats_after_game = {}
        batting_df = pd.concat([ self.home_team_batting_df, self.away_team_batting_df ])
        if "Batting" not in batting_df.columns:
            self.log(f"Batting not found in batting_df for game {self.id}", error=True)
            return None

        batting_df = batting_df[ (batting_df["Position"] != "P") & (batting_df["Batting"] != "Team") ].set_index("Batting")
        for batter in batting_df.index:
            stats_after_game[batter] = batting_df.loc[batter].to_dict()

        return stats_after_game

    def get_home_hitter_stats(self):
        batting_df = self.home_team_batting_df
        if "Batting" not in batting_df.columns:
            self.log(f"Batting not found in batting_df for game {self.id}", error=True)
            return None
        stats_after_game = {}
        batting_df = batting_df[ (batting_df["Position"] != "P") & (batting_df["Batting"] != "Team") ].set_index("Batting")
        for batter in batting_df.index:
            stats_after_game[batter] = batting_df.loc[batter].to_dict()
        return stats_after_game

    def get_away_hitter_stats(self):
        batting_df = self.away_team_batting_df
        if "Batting" not in batting_df.columns:
            self.log(f"Batting not found in batting_df for game {self.id}", error=True)
            return None
        stats_after_game = {}
        batting_df = batting_df[ (batting_df["Position"] != "P") & (batting_df["Batting"] != "Team") ].set_index("Batting")
        for batter in batting_df.index:
            stats_after_game[batter] = batting_df.loc[batter].to_dict()
        return stats_after_game

    def get_hitters(self):
        batting_df = pd.concat([ self.home_team_batting_df, self.away_team_batting_df ])
        if "Batting" not in batting_df.columns:
            self.log(f"Batting not found in batting_df for game {self.id}", error=True)
            return None

        batting_df = batting_df[ (batting_df["Position"] != "P") & (batting_df["Batting"] != "Team") ].set_index("Batting")
        return list(batting_df.index)

    def get_pitcher_stats_from_raw_data(self):
        """
        Returns stats for pitchers based on what has happened in the game + any games before it.
        stats_before_game is df with index of player names/IDs.
        """
        pitching_df = pd.concat([ self.home_team_pitching_df.iloc[:1], self.away_team_pitching_df.iloc[:1] ])
        if "Pitching" not in pitching_df.columns:
            self.log(f"Pitching not found in pitching_df for game {self.id}", error=True)
            return None
        pitching_df = pitching_df[ pitching_df["Pitching"] != "Team Totals" ].set_index("Pitching")

        stats_after_game = {}

        for pitcher in pitching_df.index:
            stats_after_game[pitcher] = pitching_df.loc[pitcher].to_dict()

        return stats_after_game

    def get_home_pitcher_stats(self):
        return self.home_team_pitching_df.iloc[0].to_dict()

    def get_away_pitcher_stats(self):
        return self.away_team_pitching_df.iloc[0].to_dict()

    def get_home_pitcher(self):
        return self.home_team_pitching_df["Pitching"][0]

    def get_away_pitcher(self):
        return self.away_team_pitching_df["Pitching"][0]

    def get_starting_pitchers(self):
        pitching_df = pd.concat([ self.home_team_pitching_df.iloc[:1], self.away_team_pitching_df.iloc[:1] ])
        if "Pitching" not in pitching_df.columns:
            self.log(f"Pitching not found in pitching_df for game {self.id}", error=True)
            return None

        pitching_df = pitching_df[ pitching_df["Pitching"] != "Team Totals" ].set_index("Pitching")
        return list(pitching_df.index)

    def get_all_pitchers(self):
        pitching_df = pd.concat([ self.home_team_pitching_df, self.away_team_pitching_df ])
        if "Pitching" not in pitching_df.columns:
            self.log(f"Pitching not found in pitching_df for game {self.id}", error=True)
            return None

        pitching_df = pitching_df[ pitching_df["Pitching"] != "Team Totals" ].set_index("Pitching")
        return list(pitching_df.index)

