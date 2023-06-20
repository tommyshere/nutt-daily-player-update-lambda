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


def main(*_, **__):
    match_day = dt.datetime.today().weekday()
    database = DATABASE_DAY[match_day + 1]

    tournaments = _select_tournaments(dt.datetime.now().strftime("%Y-%m-%d"))

    for tournament in tournaments:
        tournament_id = tournament[0]
        game_id = tournament[1]

        leaderboard_url = f"{URL}/leaderboard/{tournament_id}"
        response = requests.get(leaderboard_url, headers=HEADERS)
        json_results = response.json()
        results = json_results.get("results")

        leaderboard = results.get('leaderboard')
        top_ten = leaderboard[0:10]

        _insert_golferlookup(top_ten, tournament_id)

        users = _select_users(tournament_id)
        for user in users:
            user_id = user[0]
            values = [
                (game_id, user_id, golfer.get('player_id'), tournament_id)
                for golfer in top_ten
            ]
            _insert_rankings(database, values)


def _select_tournaments(datetime):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port="5432",
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    query = """
        SELECT tl.tournament_id, g.game_id
        FROM public.tournament_lookup tl
        INNER JOIN public.game g
        ON g.tournament_id = tl.tournament_id
        WHERE start_datetime <= %s
        AND end_datetime >= %s
    """
    cur.execute(query, (datetime, datetime))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


def _select_users(tournament_id):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port="5432",
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    query = """
        SELECT a.user_id
        FROM gameuser a
        LEFT JOIN game b 
        ON a.game_id = b.game_id 
        WHERE b.tournament_id = %s
    """
    cur.execute(query, (tournament_id,))
    rows = cur.fetchall()

    cur.close()
    conn.close()

    return rows


def _insert_golferlookup(golfers, tournament_id):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port="5432",
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()
    query = """INSERT INTO public.golfer_lookup
        (golfer_id, golfer_first_name, golfer_last_name, tournament_id) 
        VALUES 
        (%s, %s, %s, %s)
        ON CONFLICT (golfer_id, tournament_id)
        DO NOTHING
        """

    for golfer in golfers:
        cur.execute(query, (golfer.get("player_id"), golfer.get("first_name"), golfer.get("last_name"), tournament_id))

    conn.commit()

    cur.close()
    conn.close()


def _insert_rankings(database, values):
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port="5432",
        database=os.getenv("POSTGRES_DATABASE"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor()

    query = f"INSERT INTO {database} (game_id, user_id, golfer_id, tournament_id) " \
            f"VALUES (%s, %s, %s, %s);"

    for value in values:
        cur.execute(query, value)

    conn.commit()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
