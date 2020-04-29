import mysql.connector
import mysql.connector.errors
import requests

IMDB_ID_MAX = 9916880

CHECK_FOR_ID_QUERY = """SELECT * 
                            FROM media 
                            WHERE imdb_id=%s"""

ADD_MEDIA_SERIES = """INSERT INTO media
                    (title, year, rated, released, runtime, genre, director, writer, actors, plot, language, country, 
                    awards, poster, metascore, imdb_rating, imdb_votes, imdb_id, type, total_seasons)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

ADD_MEDIA_MOVIE = """INSERT INTO media
                    (title, year, rated, released, runtime, genre, director, writer, actors, plot, language, country, 
                    awards, poster, metascore, imdb_rating, imdb_votes, imdb_id, type, dvd, box_office, production, website)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

ADD_MEDIA_EPISODE = """INSERT INTO media
                    (title, year, rated, released, runtime, genre, director, writer, actors, plot, language, country, 
                    awards, poster, metascore, imdb_rating, imdb_votes, imdb_id, type, season, episode, series_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

ADD_MEDIA_RATINGS = """INSERT INTO media_rating
                                (source, value, media_id)
                                VALUES (%s, %s, %s)"""

# get key at http://www.omdbapi.com/
KEY = ""
URL = "http://www.omdbapi.com/"


def get_connection():
    connection = mysql.connector.connect(host='localhost',
                                         database='full-stack-entertainment',
                                         user='entertainmentDb',
                                         password='entertainmentDb')
    return connection


def get_check_point():
    with open("check_point.txt", "r") as check_point:
        check_point = int(check_point.readline())
        return check_point


def set_check_point(check_point_value):
    with open("check_point.txt", "w") as check_point:
        check_point.write(str(check_point_value))


def get_data(url, params, imdb_id):
    params["i"] = imdb_id
    r = requests.get(url, params=params)
    try:
        data = r.json()
        print("ID: {} :: Data: {}".format(str(imdb_id), data))
    except ValueError:
        print("Trouble getting data")
        data = -1

    if data["Response"] == "False":
        print("Media Id not valid")
        data = -1

    return data


def set_up_table_data_insert_queries(data):
    media_data_basic = (
        data["Title"], data["Year"], data["Rated"], data["Released"], data["Runtime"], data["Genre"],
        data["Director"], data["Writer"], data["Actors"], data["Plot"], data["Language"], data["Country"],
        data["Awards"], data["Poster"], data["Metascore"], data["imdbRating"], data["imdbVotes"], data["imdbID"],
        data["Type"])
    if data["Type"] in ["movie", "game"]:
        media_data_movie = (data["DVD"], data["BoxOffice"], data["Production"], data["Website"])
        media_data_insert_query = media_data_basic + media_data_movie
        add_media_query = ADD_MEDIA_MOVIE
    elif data["Type"] == "series":
        media_data_series = (data["totalSeasons"],)
        media_data_insert_query = media_data_basic + media_data_series
        add_media_query = ADD_MEDIA_SERIES
    else:
        media_data_episode = (data["Season"], data["Episode"], data["seriesID"])
        media_data_insert_query = media_data_basic + media_data_episode
        add_media_query = ADD_MEDIA_EPISODE

    return add_media_query, media_data_insert_query


def insert_media_data(cursor, data):
    add_media_query, media_data_insert_query = set_up_table_data_insert_queries(data)
    cursor.execute(add_media_query, media_data_insert_query)
    media_id = cursor.getlastrowid()
    insert_ratings(cursor, media_id, data)


def insert_ratings(cursor, media_id, data):
    for rating in data["Ratings"]:
        media_rating_data = (rating["Source"], rating["Value"], str(media_id))
        cursor.execute(ADD_MEDIA_RATINGS, media_rating_data)


def run():
    params = {"apikey": KEY, "i": ""}
    data_success_count = 1

    connection = get_connection()
    cursor = connection.cursor(buffered=True)

    start = get_check_point() + 1
    for temp_id in range(start, IMDB_ID_MAX):

        imdb_id = "tt" + ('%0*d' % (7, temp_id))

        cursor.execute(CHECK_FOR_ID_QUERY, (imdb_id,))
        media_not_in_table = cursor.rowcount == 0

        if media_not_in_table:
            try:
                data = get_data(URL, params, imdb_id)
                if data == -1:
                    continue
                insert_media_data(cursor, data)
                data_success_count += 1
            except mysql.connector.Error and TypeError:
                print("Error entering data into table")
                continue

            if data_success_count % 100 == 0:
                connection.commit()
                print("Entries Committed")
                set_check_point(temp_id)

        else:
            print("Media already added to table")

    cursor.close()
    connection.commit()
    print("Final commit completed")
    connection.close()


if __name__ == "__main__":
    run()
