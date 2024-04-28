import os
import sys
import pickle
import statsapi
import tqdm
import requests
import pandas as pd
import numpy as np
from pymongo import MongoClient

from dotenv import load_dotenv
load_dotenv()

sys.path.append("utils")
from base_class import BaseClass

def get_database():
    client = MongoClient(os.getenv("MONGO_URL"))
    return client["home_run_data"]

class SportsbookOddsDataHandler(BaseClass):
    def __init__(self):
        self.odds_api_key = os.getenv("ODDS_API_KEY")

    def get_games_to_update(self, date=None, threshold_minutes=15, update_all_games=False):
        # We want to update games 15 minutes or so before they start
        if date is None:
            schedule = statsapi.schedule()
        else:
            schedule = statsapi.schedule(pd.Timestamp(date).strftime("%Y-%m-%d"))
        threshold = pd.Timedelta(f"{threshold_minutes} minutes")
        games_to_update = []
        for game in schedule:
            if game["status"] not in ["Pre-Game", "Warmup", "Scheduled"]:
                continue
            self.log(f"Processing game {game['game_id']} at {game['game_datetime']}")
            game_time = pd.Timestamp(game["game_datetime"]).tz_convert("GMT")
            current_time = pd.Timestamp.now().tz_localize("America/Chicago").tz_convert("GMT")
            if update_all_games and game_time > current_time:
                self.log(f"Game {game['game_id']} at {game['game_datetime']} added to games to updates")
                games_to_update.append(game)
            elif game_time - current_time < threshold:
                self.log(f"Game {game['game_id']} at {game['game_datetime']} added to games to updates")
                games_to_update.append(game)
        return games_to_update

    def get_odds_for_event(self, event_id):
        url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events/{event_id}/odds?apiKey={self.odds_api_key}&regions=us&markets=batter_home_runs&oddsFormat=american"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            self.log(f"Found odds for event {event_id}")
            return data
        except requests.exceptions.RequestException as e:
            self.log(f"Error fetching MLB odds: {e}", error=True)
            return None

    def convert_integer_to_american_odds_string(self, odds):
        if odds > 0:
            return f"+{odds}"
        elif odds < 0:
            return str(odds)
        else:
            return "+0"

    def get_odds_api_events(self):
        url = f"https://api.the-odds-api.com/v4/sports/baseball_mlb/events?apiKey={self.odds_api_key}"
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            self.log(f"Found {len(data)} Odds API MLB events")
            return data
        except requests.exceptions.RequestException as e:
            self.log(f"Error fetching MLB odds: {e}", error=True)
            return None

    def get_odds_for_games(self, games, accepted_sportsbooks):
        events_data = self.get_odds_api_events()
        if events_data is None:
            return None

        ret = []
        for event in events_data:
            # Check if event is in the input games to fetch
            should_skip = True
            for game_to_fetch in games:
                current_time = pd.Timestamp.now().tz_localize("America/Chicago").tz_convert("GMT")
                threshold = pd.Timedelta("10 minutes")
                if pd.Timestamp(event["commence_time"]) + threshold > pd.Timestamp(game_to_fetch["game_datetime"]) and\
                        pd.Timestamp(event["commence_time"]) - threshold < pd.Timestamp(game_to_fetch["game_datetime"]) and\
                        event["home_team"] == game_to_fetch["home_name"] and\
                        event["away_team"] == game_to_fetch["away_name"]:
                    should_skip = False
            if should_skip:
                continue

            self.log(f"Getting odds from Odds API for event {event['id']}")
            event_odds = self.get_odds_for_event(event["id"])
            over_or_under = ""
            if event_odds is not None:
                for book in accepted_sportsbooks:
                    book_data = None
                    for bm in event_odds["bookmakers"]:
                        if bm["key"] == book:
                            book_data = bm
                            break
                    if book_data is None:
                        self.log(f"Could not find {book} data in event odds for {event['id']}")
                        continue

                    assert(len(book_data["markets"]) == 1)
                    for market in book_data["markets"][0]["outcomes"]:
                        if market["point"] != 0.5:
                            continue
                        over_or_under = market["name"].lower()
                        player_name = market["description"]
                        odds = self.convert_integer_to_american_odds_string(market["price"])
                        ret.append({
                            "player_name": player_name,
                            "point": 0.5,
                            "sportsbook": book,
                            "over_or_under": over_or_under,
                            "odds": odds,
                            "utc_update_time": str(pd.Timestamp.utcnow()),
                            "game_time": str(pd.Timestamp(event["commence_time"])),
                        })
        return ret

    def upload_results_to_db(self, odds_updates, collection):
        # First, we need to aggregate the results by player
        all_player_names = list(set([x["player_name"] for x in odds_updates]))
        for player in all_player_names:
            player_updates = [x for x in odds_updates if x["player_name"] == player]
            player_update_sportsbooks = list(set([x["sportsbook"] for x in player_updates]))
            odds_object = {x: {} for x in player_update_sportsbooks}
            for update in player_updates:
                odds_object[update["sportsbook"]][update["over_or_under"]] = update["odds"]

            odds_data = {
                "data": odds_object,
                "update_time": player_updates[0]["utc_update_time"],
                "game_time": player_updates[0]["game_time"]
            }

            game_date = pd.Timestamp(player_updates[0]["game_time"]).tz_convert("America/New_York").strftime("%Y-%m-%d")
            queried_items = collection.find({"player_name": player,
                                              "date": game_date,
                                              })
            if queried_items is not None:
                for queried_item in queried_items:
                    new_item = queried_item.copy()
                    new_item["odds_data"] = odds_data
                    did_update = False
                    if "odds_data" not in queried_item or queried_item["odds_data"] != odds_data:
                        collection.replace_one(queried_item, new_item)
                        self.log(f"Updating {new_item['player_name']} {new_item['date']} {new_item['model']} {new_item['did_hit_hr']} {new_item['home_run_odds']}")
                        did_update = True
                    if not did_update:
                        self.log(f"No change for {new_item['player_name']} {new_item['date']} {new_item['model']} {new_item['did_hit_hr']} {new_item['home_run_odds']}")
            else:
                self.log(f"Cannot find {player} in database. No odds data update performed.")

ACCEPTED_SPORTSBOOKS = ["draftkings", "fanduel", "pointsbetus", "betrivers"]
if __name__ == "__main__":
    h = SportsbookOddsDataHandler()
    h.log("Loaded handler")

    db = get_database()
    collection = db["data"]
    h.log("Connected to db and collection")

    games_to_update = h.get_games_to_update(threshold_minutes=60, update_all_games=True)
    h.log(f"Updating {len(games_to_update)} games")
    if len(games_to_update) > 0:
        odds_for_games = h.get_odds_for_games(games_to_update, ACCEPTED_SPORTSBOOKS)
        with open("odds_for_games.p", "wb") as f:
            pickle.dump(odds_for_games, f)
        # with open("odds_for_games.p", "rb") as f:
        #     odds_for_games = pickle.load(f)
        if len(odds_for_games) > 0:
            h.upload_results_to_db(odds_for_games, collection)

