
SEASON_START_DATE="2024-03-28"
python main.py --download $SEASON_START_DATE $(date +%F) ./game_data/2024/
python main.py --get_updates $SEASON_START_DATE $(date +%F) ./game_data/2024/ ./update_data/updates.json
python main.py --push_to_db ./update_data/updates.json
