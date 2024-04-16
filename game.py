import sys
import os
import pickle
import pandas as pd
import numpy as np

sys.path.append("utils")
from base_class import BaseClass
from player import Player

class Game(BaseClass):
    def __init__(
                self,
                id_,
                time,
                date,
                venue,
                home_team,
                away_team,
                home_team_batting_df,
                away_team_batting_df,
                home_team_pitching_df,
                away_team_pitching_df,
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

    def save(self, data_dir="./data/game_data"):
        filename = os.path.join(data_dir, self.id + ".p")
        self.log(f"Saving game {self.id} to {filename}")
        with open(filename, "wb") as f:
            pickle.dump(self, f)

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

    def get_hitters(self):
        batting_df = pd.concat([ self.home_team_batting_df, self.away_team_batting_df ])
        if "Batting" not in batting_df.columns:
            self.log(f"Batting not found in batting_df for game {self.id}", error=True)
            return None

        batting_df = batting_df[ (batting_df["Position"] != "P") & (batting_df["Batting"] != "Team") ].set_index("Batting")
        return list(batting_df.index)

