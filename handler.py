import requests
import psycopg2
import datetime as dt
import os


URL = os.getenv("API_ENDPOINT")

HEADERS = {
    "X-RapidAPI-Key": os.getenv("RAPID_API_KEY"),
    "X-RapidAPI-Host": os.getenv("RAPID_API_HOST"),
}

DATABASE_DAY = {
    3: "day_one",  # Thursday
    4: "day_two",  # Friday
    5: "day_three",  # Saturday
    6: "day_four",  # Sunday
}


def main():
    match_day = dt.datetime.today().weekday()
    database = DATABASE_DAY[match_day + 1]

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port="5432",
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()
    tournaments = _select_tournaments(cur, dt.datetime.now().strftime("%Y-%m-%d"))

    for tournament in tournaments:
        tournament_id = tournament[0]
        leaderboard_url = f"{URL}/leaderboard/{tournament_id}"
        response = requests.get(leaderboard_url, headers=HEADERS)
        json_results = response.json()
        results = json_results.get("results")

        leaderboard = results.get('leaderboard')
        top_ten = leaderboard[0:10]

        users = _select_users(cur, tournament_id)
        for user in users:
            user_id = user[0]
            game_id = user[1]
            for golfer in top_ten:
                golfer_id = golfer.get('player_id')
                golfer_name = f"{golfer.get('first_name')} {golfer.get('last_name')}"
                _insert_rankings(cur, database, game_id, user_id, golfer_id, golfer_name, tournament_id)

            conn.commit()

    cur.close()
    conn.close()


def _select_tournaments(cur, datetime):
    query = """
        SELECT tournament_id
        FROM public.tournament_lookup
        WHERE start_datetime <= %s
        AND end_datetime >= %s
    """
    cur.execute(query, (datetime, datetime))
    rows = cur.fetchall()
    return rows


def _select_users(cur, tournament_id):
    query = """
        SELECT a.user_id, a.game_id
        FROM gameuser a
        LEFT JOIN game b 
        ON a.game_id = b.game_id 
        WHERE b.tournament_id = %s
    """
    cur.execute(query, (tournament_id,))
    rows = cur.fetchall()
    return rows


def _insert_rankings(cur, database, game_id, user_id, golfer_id, golfer_name, tournament_id):
    query = f"INSERT INTO {database} (game_id, user_id, golfer_id, golfer_name, tournament_id) " \
            f"VALUES (%s, %s, %s, %s, %s);"
    cur.execute(query, (game_id, user_id, golfer_id, golfer_name, tournament_id))


if __name__ == "__main__":
    main()
