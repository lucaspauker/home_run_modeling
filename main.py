import os
import argparse
import pickle
import tqdm
import json
import pandas as pd
import numpy as np
import statsapi
from pymongo import MongoClient
from dotenv import load_dotenv
load_dotenv()
from scraper import BaseballReferenceScraper
from sportsbook_odds_data_handler import SportsbookOddsDataHandler
from runner import Runner
from config.models import models

STAT_NAMES = ["Batting Average", "On-Base%", "Slugging %", "At Bats", "Home Runs", "Runs Batted In",\
              "Average Home Runs", "Average Runs Batted In", "At Bats Per Game", "Games Played", "details"]
MIN_ABS_TO_PUSH = 50
ACCEPTED_SPORTSBOOKS = ["draftkings", "fanduel", "pointsbetus", "betrivers"]

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def log(text, error=False, log=True, verbose=True):
    msg = f"[{pd.Timestamp.now()}] {text}"
    if error:
        msg = f"[{pd.Timestamp.now()}] ERROR: {text}"
    if verbose:
        print(msg)
    if log:
        logfile = "logs/" + pd.Timestamp.now().strftime("%Y%m%d") + ".log"
        with open(logfile, "a") as f:
            f.write(msg + "\n")

def download(start_date, end_date, data_dir, remove=False):
    s = BaseballReferenceScraper(data_dir=data_dir)

    if remove:
        wildcard = os.path.join(data_dir, "*")
        s.log(f"Removing {wildcard}")
        for f in glob.glob(wildcard):
            os.remove(f)

    game_ids = s.get_game_ids(start_date, end_date)
    for game_id in game_ids:
        try:
            s.get_game_data(game_id)
        except:
            log("Error", error=True)
            continue

def get_database():
    client = MongoClient(os.getenv("MONGO_URL"))
    return client["home_run_data"]

def add_item(collection, item):
    required_fields = ["player_name", "date", "model", "home_run_odds", "did_hit_hr"]
    for field in required_fields:
        if field not in item:
            log(f"{field} not in item", error=True)
            return False

    # Check if item already exists
    queried_item = collection.find_one({"player_name": item["player_name"],
                                        "date": item["date"],
                                        "model": item["model"],
                                        })
    if queried_item is not None:
        item["_id"] = queried_item["_id"]
        if "odds_data" in queried_item:
            item["odds_data"] = queried_item["odds_data"]
        did_update = False
        for field in required_fields:
            if item[field] != queried_item[field]:
                collection.replace_one(queried_item, item)
                log(f"Updating {item['player_name']} {item['date']} {item['model']} {item['did_hit_hr']} {item['home_run_odds']}")
                did_update = True
                break
        if not did_update:
            log(f"No change for {item['player_name']} {item['date']} {item['model']} {item['did_hit_hr']} {item['home_run_odds']}")
    else:
        collection.insert_one(item)
        log(f"Added {item['player_name']} {item['date']} {item['model']} {item['did_hit_hr']} {item['home_run_odds']}")
    return True

def add_data(collection, data_to_add):
    for item in data_to_add:
        if not add_item(collection, item):
            pass

def date_greater_than_or_equal(d1, d2):
    return d1.year > d2.year or\
           (d1.year == d2.year and d1.month > d2.month) or\
           (d1.year == d2.year and d1.month == d2.month and d1.day >= d2.day)

if __name__ == "__main__":
    log("----Running main-----")
    parser = argparse.ArgumentParser(description="Baseball modeling CLI")

    parser.add_argument("--download", nargs="+", help="Download data")
    parser.add_argument("--get_updates", nargs="+", help="Get updates for model results for database")
    parser.add_argument("--get_updates_today", nargs="+", help="Get updates for model results today's games")
    parser.add_argument("--push_to_db", nargs="+", help="Push updates to MongoDB")
    parser.add_argument("--update_sportsbook_odds", nargs="+", help="Push sportsbook odds updates to MongoDB")
    args = parser.parse_args()

    if args.download is not None:
        assert(len(args.download) >= 3)
        start_date = args.download[0]
        end_date = args.download[1]
        data_dir = args.download[2]
        remove = False
        if len(args.download) > 3:
            remove = args.download[3]

        log(f"Running download mode from {start_date} to {end_date} and saving into {data_dir}")

        if not os.path.exists(data_dir):
            log(f"{data_dir} does not exist", error=True)
            assert(False)

        download(start_date, end_date, data_dir, remove=remove)

    if args.get_updates is not None:
        assert(len(args.get_updates) >= 4)
        start_date = args.get_updates[0]
        end_date = args.get_updates[1]
        data_dir = args.get_updates[2]
        output_file = args.get_updates[3]

        log(f"Running update mode from {start_date} to {end_date} and saving into {data_dir}")

        r = Runner(STAT_NAMES, data_dir=data_dir)
        r.build_player_map_for_all_games()

        for model_config in models:
            log(f"Getting updates for model {model_config['name']}")
            model_path = model_config["model_path"]
            scaler_path = model_config["scaler_path"]

            with open(model_path, "rb") as f:
                model = pickle.load(f)
            with open(scaler_path, "rb") as f:
                scaler = pickle.load(f)

            items = []
            for game_id in tqdm.tqdm(r.get_games()):
                game = r.get_game(game_id)
                if date_greater_than_or_equal(pd.Timestamp(game.date), pd.Timestamp(start_date)) and date_greater_than_or_equal(pd.Timestamp(end_date), pd.Timestamp(game.date)):
                    for player_name in game.get_hitters():
                        stats = r.player_map.get_player(player_name).get_stats_before_game(game_id, game.date, num_games_threshold=0)
                        if stats is not None and stats["At Bats"] < MIN_ABS_TO_PUSH:
                            log(f"Not enough ABs ({stats['At Bats']}) for {player_name}, skipping")
                            continue
                        input_data = scaler.transform([np.array(stats[model_config["features"]]).astype(float)])
                        predicted_prob = model.predict_proba(input_data)[0][1]
                        did_hit_home_run = r.player_map.get_player(player_name).did_hit_home_run(game_id)
                        if did_hit_home_run is None:
                            c = 2
                        elif did_hit_home_run:
                            c = 1
                        else:
                            c = 0
                        item = {
                            "player_name": player_name,
                            "date": game.date.strftime("%Y-%m-%d"),
                            "model": model_config["name"],
                            "home_run_odds": predicted_prob,
                            "did_hit_hr": c,
                            "stats": dict(stats[["Batting Average", "Home Runs", "Runs Batted In", "On-Base%", "Slugging %", "At Bats", "Games Played"]]),
                            "game_id": game_id,
                        }
                        items.append(item)

        with open(output_file, "w") as f:
            json.dump(items, f, cls=NpEncoder)

    if args.get_updates_today is not None:
        assert(len(args.get_updates_today) >= 2)
        output_file = args.get_updates_today[0]
        data_dir = args.get_updates_today[1]

        schedule = statsapi.schedule()
        game_ids = [x["game_id"] for x in schedule]
        log(f"Getting updates for {len(game_ids)} games")

        batter_names, batter_teams = [], []
        for game_id in tqdm.tqdm(game_ids):
            boxscore_data = statsapi.boxscore_data(game_id)
            batter_ids = [x["personId"] for x in boxscore_data["awayBatters"] if x["personId"] != 0]
            batter_ids += [x["personId"] for x in boxscore_data["homeBatters"] if x["personId"] != 0]
            for bid in batter_ids:
                player_query = statsapi.lookup_player(bid)[0]["nameFirstLast"]
                player_team = statsapi.lookup_team(player_query["currentTeam"]["id"])[0]
                batter_names.append(player_name)
                batter_teams.append(player_team)
        log(f"Found {len(batter_names)} batters today")

        r = Runner(STAT_NAMES, data_dir=data_dir)
        r.build_player_map_for_all_games()

        for model_config in models:
            log(f"Getting updates for model {model_config['name']}")
            model_path = model_config["model_path"]
            scaler_path = model_config["scaler_path"]

            with open(model_path, "rb") as f:
                model = pickle.load(f)
            with open(scaler_path, "rb") as f:
                scaler = pickle.load(f)

            items = []
            for player_name, player_team in zip(batter_names, batter_teams):
                if r.player_map.get_player(player_name) is None:
                    continue
                stats = r.player_map.get_player(player_name).get_latest_stats()
                if stats is not None and stats["At Bats"] < MIN_ABS_TO_PUSH:
                    log(f"Not enough ABs ({stats['At Bats']}) for {player_name}, skipping")
                    continue
                input_data = scaler.transform([np.array(stats[model_config["features"]]).astype(float)])
                predicted_prob = model.predict_proba(input_data)[0][1]
                did_hit_home_run = r.player_map.get_player(player_name).did_hit_home_run(game_id)
                assert(did_hit_home_run is None)
                if did_hit_home_run is None:
                    c = 2
                elif did_hit_home_run:
                    c = 1
                else:
                    c = 0
                item = {
                    "player_name": player_name,
                    "current_team": player_team,
                    "date": pd.Timestamp.now().strftime("%Y-%m-%d"),
                    "model": model_config["name"],
                    "home_run_odds": predicted_prob,
                    "did_hit_hr": c,
                    "stats": dict(stats[["Batting Average", "Home Runs", "Runs Batted In", "On-Base%", "Slugging %", "At Bats", "Games Played"]]),
                    "game_id": -1,
                }
                items.append(item)

        with open(output_file, "w") as f:
            json.dump(items, f, cls=NpEncoder)

    if args.push_to_db is not None:
        assert(len(args.push_to_db) >= 1)
        output_file = args.push_to_db[0]

        if not os.path.exists(output_file):
            log(f"{output_file} does not exist", error=True)
            assert(False)

        log(f"Running push to DB mode from {output_file}")

        db = get_database()
        collection = db["data"]
        log("Connected to db and collection")

        with open(output_file, "r") as f:
            data_to_add = json.load(f)
            add_data(collection, data_to_add)

    if args.update_sportsbook_odds is not None:
        assert(len(args.update_sportsbook_odds) >= 1)
        threshold_minutes = int(args.update_sportsbook_odds[0])

        h = SportsbookOddsDataHandler()
        h.log("Loaded handler")
        if threshold_minutes >= 0:
            games_to_update = h.get_games_to_update(threshold_minutes=threshold_minutes, update_all_games=False)
        else:
            games_to_update = h.get_games_to_update(update_all_games=True)
        h.log(f"Updating {len(games_to_update)} games")
        if len(games_to_update) > 0:
            odds_for_games = h.get_odds_for_games(games_to_update, ACCEPTED_SPORTSBOOKS)
            if len(odds_for_games) > 0:
                db = get_database()
                collection = db["data"]
                h.log("Connected to db and collection")
                h.upload_results_to_db(odds_for_games, collection)


