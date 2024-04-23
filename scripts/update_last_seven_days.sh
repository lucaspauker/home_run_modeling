#!/bin/bash

SEASON_START_DATE="2024-03-28"

# Get the current date components
current_day=$(date +%d)
current_month=$(date +%m)
current_year=$(date +%Y)

# Get the season start date components
season_start_year=${SEASON_START_DATE:0:4}
season_start_month=${SEASON_START_DATE:5:2}
season_start_day=${SEASON_START_DATE:8:2}

# Calculate the date from one week ago
one_week_ago_day=$((current_day - 7))
one_week_ago_month=$current_month
one_week_ago_year=$current_year

# Check if the date from one week ago is before the season start date
if [[ "$one_week_ago_year$one_week_ago_month$one_week_ago_day" < "$season_start_year$season_start_month$season_start_day" ]]; then
    one_week_ago_day=$season_start_day
    one_week_ago_month=$season_start_month
    one_week_ago_year=$season_start_year
fi

# Adjust the date components if the calculated day is less than 1
if [ $one_week_ago_day -lt 1 ]; then
    # Subtract 1 from the month
    one_week_ago_month=$((current_month - 1))
    # Handle the case when the previous month is December
    if [ $one_week_ago_month -eq 0 ]; then
        one_week_ago_month=12
        one_week_ago_year=$((current_year - 1))
    fi
    # Calculate the adjusted day
    days_in_previous_month=$(cal $one_week_ago_month $one_week_ago_year | awk 'NF {DAYS = $NF}; END {print DAYS}')
    one_week_ago_day=$((days_in_previous_month + one_week_ago_day))
fi
DATE_ONE_WEEK_AGO=$(printf "%d-%02d-%02d" $one_week_ago_year $one_week_ago_month $one_week_ago_day)
CURRENT_DATE=$(date +%F)

python main.py --download $DATE_ONE_WEEK_AGO $CURRENT_DATE  ./game_data/2024/
python main.py --get_updates_today ./update_data/updates_today.json ./game_data/2024/
python main.py --push_to_db ./update_data/updates_today.json
python main.py --get_updates $DATE_ONE_WEEK_AGO $CURRENT_DATE ./game_data/2024/ ./update_data/updates.json
python main.py --push_to_db ./update_data/updates.json
