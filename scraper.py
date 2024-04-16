import sys
import os
import time
import requests
import warnings
import datetime
import pandas as pd
import numpy as np

from bs4 import BeautifulSoup, Comment, Tag, MarkupResemblesLocatorWarning
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

sys.path.append("utils")
from base_class import BaseClass
from game import Game

class BaseballReferenceScraper(BaseClass):
    def __init__(self, data_dir="./data/game_data"):
        self.base_url = "https://www.baseball-reference.com/"
        self.headers = {"User-Agent": "User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Safari/537.36"}
        self.data_dir = data_dir

    def get_response(self, link, n_tries=5):
        for i in range(n_tries):
            res = requests.get(link, headers=self.headers)
            time.sleep(3)  # Sleep after request to avoid rate limits (20 req/min) https://www.sports-reference.com/bot-traffic.html
            if res.status_code != 200:
                if i < n_tries - 1:
                    self.log(f"Request for {link} failed, trying again", error=True)
                continue
            else:
                return res
        raise BaseException(f"Request for {link} failed")

    def get_game_ids(self, start_time, end_time):
        game_ids = []
        for t in pd.date_range(start_time, end_time):
            self.log(f"Getting game IDs for {t.strftime('%Y/%m/%d')}")
            link = os.path.join(self.base_url, "boxes", f"?year={t.year}&month={t.month}&day={t.day}")

            res = self.get_response(link)
            soup = BeautifulSoup(res.text, "html.parser")

            for a in soup.find_all("a"):
                if a.get_text() == "Final":
                    href = a["href"]
                    game_ids.append(href.split("/")[-1].split(".shtml")[0])
        self.log(f"{len(game_ids)} game IDs found")
        return game_ids

    def game_id_to_link(self, game_id):
        return os.path.join(self.base_url, "boxes", game_id[:3], f"{game_id}.shtml")

    def html_table_to_dataframe(self, html_table):
        if isinstance(html_table, str):
            soup = BeautifulSoup(html_table, "html.parser")
        elif isinstance(html_table, Tag):
            soup = html_table
        else:
            raise ValueError("Input must be a string or a Beautiful Soup object")

        is_pitching = "Pitching" in soup.get_text()

        rows = soup.find_all("tr")
        headers = [header["aria-label"] for header in rows[0].find_all("th")]
        headers.append("Position")

        data = []
        for row in rows[1:]:
            if is_pitching:
                row_data = [cell.get_text(strip=False) for cell in row.find_all("th")]
                row_data[0] = row_data[0].split(",")[0]
                row_data += [cell.get_text(strip=True) for cell in row.find_all("td")]
                position = "P"
                row_data += [position]
            else:
                row_data = [" ".join(cell.get_text(strip=False).split()[:-1]) for cell in row.find_all("th")]
                row_data += [cell.get_text(strip=True) for cell in row.find_all("td")]
                try:
                    row_data += [cell.get_text(strip=False).split()[-1] for cell in row.find_all("th")]
                except IndexError:
                    continue
            data.append(row_data)

        try:
            df = pd.DataFrame(data, columns=headers)
        except ValueError:
            return None
        return df

    def get_game_data(self, game_id):
        # Check if game is already in data_dir
        path = os.path.join(self.data_dir, game_id + ".p")
        if os.path.exists(path):
            self.log(f"Path {path} exists")
            return

        link = self.game_id_to_link(game_id)
        self.log(f"Getting game data for {game_id} at {link}")

        res = self.get_response(link)
        soup = BeautifulSoup(res.text, "html.parser")

        h2s = soup.find_all("h2")
        away_team = h2s[0].get_text()
        home_team = h2s[1].get_text()

        venue = None
        time = None
        date = game_id[3:-1]
        date = datetime.date(int(date[:4]), int(date[4:6]), int(date[6:8]))
        for div in soup.find("div", class_="scorebox_meta").find_all("div"):
            if "Venue" in div.get_text():
                venue = div.get_text().split("Venue: ")[1]
            if "Start Time" in div.get_text():
                time = div.get_text().split("Start Time: ")[1].split(" Local")[0]
        if venue is None:
            raise BaseException("Venue not found")
        if time is None:
            raise BaseException("Time not found")

        table_dfs = []
        comments = soup.find_all(text=lambda text:isinstance(text, Comment))
        for comment in comments:
            comment_soup = BeautifulSoup(comment.extract(), "html.parser")
            for table in comment_soup.find_all("table"):
                if np.any([x in table.get_text() for x in ["Play by Play Table", "Top 5 Plays Table"]]):
                    continue
                table_df = self.html_table_to_dataframe(table)
                if table_df is None:
                    continue
                table_dfs.append(table_df)
        assert(len(table_dfs) == 4)
        away_team_batting_df, home_team_batting_df, away_team_pitching_df, home_team_pitching_df = table_dfs

        g = Game(
                game_id,
                time,
                date,
                venue,
                home_team,
                away_team,
                home_team_batting_df,
                away_team_batting_df,
                home_team_pitching_df,
                away_team_pitching_df,
            )
        g.save(data_dir=self.data_dir)
        return g

