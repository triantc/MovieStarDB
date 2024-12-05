import sys,os
sys.path.append(os.path.join(os.path.split(os.path.abspath(__file__))[0], 'lib'))
import pymysql

def connection():
    ''' User this function to create your connections '''    
    con = pymysql.connect(host='localhost', port=3306, user='user', passwd='password', db='newschema') #update with your settings
    
    return con

def updateRank(rank1, rank2, movieTitle):

    # Create a new connection
    con = connection()
    # Create a cursor on the connection
    cur = con.cursor()
    
    # Check rank values
    try:
        rank1 = float(rank1)
        rank2 = float(rank2)
        if not (0 <= rank1 <= 10) or not (0 <= rank2 <= 10):
            return [("status",), ("error",)]
    except ValueError:
        return [("status",), ("error",)]
    
    try:
        print(f"Fetching current rank for movie: {movieTitle}")

        # Fetch the current rank of the movie
        select_query = "SELECT `rank` FROM `movie` WHERE `title` = %s" 
        # Backticks are used for `rank` because it is a reserved keyword, and for `movie` as a best practice.
        print(f"Executing query: {select_query} with title {movieTitle}")
        cur.execute(select_query, (movieTitle,))
        results = cur.fetchall()
        
        if len(results) != 1:
            return [("status",), ("error",)]  # No movie found or multiple movies with the same title
        
        # Print the current rank
        current_rank = results[0][0]
        print(f"Current rank: {current_rank}")

        # Calculate the new rank
        if current_rank is not None:
            new_rank = (current_rank + rank1 + rank2) / 3
        else:
            new_rank = (rank1 + rank2) / 2
        
        print(f"Updating rank to {new_rank} for movie: {movieTitle}")

        # Update the rank of the movie
        update_query = "UPDATE `movie` SET `rank` = %s WHERE `title` = %s"
        print(f"Executing query: {update_query} with rank {new_rank} and title {movieTitle}")
        cur.execute(update_query, (new_rank, movieTitle))
        con.commit()

        # Fetch the updated rank of the movie
        cur.execute(select_query, (movieTitle,))
        results = cur.fetchall()
        updated_rank = results[0][0]
        print(f"Updated rank: {updated_rank}")
        
        return [("status",), ("ok",)]
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return [("status",), ("error",)]
    
    finally:
        cur.close()
        con.close()

def colleaguesOfColleagues(actorId1, actorId2):

    # Create a new connection
    con = connection()

    # Create a cursor on the connection
    # Used dict-cursor for the code to be more readable
    cur = con.cursor(pymysql.cursors.DictCursor)
    
    try:
        # Find all movies for actor a (actorId1) and actor b (actorId2)
        query_movies = "SELECT movie_id FROM role WHERE actor_id = %s"
        
        cur.execute(query_movies, (actorId1,))
        movies_a = [row['movie_id'] for row in cur.fetchall()]
        if not movies_a:
            return [("status",), ("error",)]  # No movies found for actor a
        
        cur.execute(query_movies, (actorId2,))
        movies_b = [row['movie_id'] for row in cur.fetchall()]
        if not movies_b:
            return [("status",), ("error",)]  # No movies found for actor b
        
        # Find all actors who acted with actor a and actor b
        query_actors = "SELECT DISTINCT actor_id FROM role WHERE movie_id IN %s AND actor_id != %s"
        
        cur.execute(query_actors, (movies_a, actorId1))
        actors_c = [row['actor_id'] for row in cur.fetchall()]
        if not actors_c:
            return [("status",), ("error",)]  # No actors found who acted with actor a
        
        cur.execute(query_actors, (movies_b, actorId2))
        actors_d = [row['actor_id'] for row in cur.fetchall()]
        if not actors_d:
            return [("status",), ("error",)]  # No actors found who acted with actor b
        
        # Find movies where each pair (c, d) acted together
        query_movies_cd = "SELECT movie_id FROM role WHERE actor_id = %s"
        
        results = []
        for actor_c in actors_c:
            cur.execute(query_movies_cd, (actor_c,))
            movies_c = [row['movie_id'] for row in cur.fetchall()]
            
            for actor_d in actors_d:
                cur.execute(query_movies_cd, (actor_d,))
                movies_d = [row['movie_id'] for row in cur.fetchall()]
                
                common_movies = set(movies_c) & set(movies_d)
                for movie_id in common_movies:
                    cur.execute("SELECT title FROM movie WHERE movie_id = %s", (movie_id,))
                    movie_title = cur.fetchone()['title']
                    results.append((movie_title, actor_c, actor_d, actorId1, actorId2))
        
        if not results:
            return [("status",), ("error",)]  # No common movies found for pairs (c, d)
        
        output = [("movieTitle", "colleagueOfActor1", "colleagueOfActor2", "actor1","actor2",),]
        output.extend(results)
        
        return output
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return [("status",), ("error",)]
    
    finally:
        cur.close()
        con.close()

def actorPairs(actorId):

    # Create a new connection
    con = connection()
    
    # Create a cursor on the connection
    cur = con.cursor()
    
    try:
        # Find all movies for the given actor
        query_movies = "SELECT DISTINCT movie_id FROM role WHERE actor_id = %s"
        cur.execute(query_movies, (actorId,))
        movies = [row[0] for row in cur.fetchall()]
        
        if not movies:
            return [("status",), ("error",)]  # No movies found for the given actor
        
        # Find actors with whom the given actor has acted in at least seven different genres
        valid_coactors = {}
        for movie_id in movies:
            # Find genres of the current movie
            query_genres = "SELECT genre_id FROM movie_has_genre WHERE movie_id = %s"
            cur.execute(query_genres, (movie_id,))
            genres = {row[0] for row in cur.fetchall()}
            
            # Find co-actors for the current movie
            query_coactors = """
                SELECT DISTINCT actor_id
                FROM role
                WHERE movie_id = %s
                AND actor_id != %s
            """
            cur.execute(query_coactors, (movie_id, actorId))
            coactors = [row[0] for row in cur.fetchall()]
            
            for coactor in coactors:
                if coactor not in valid_coactors:
                    valid_coactors[coactor] = set()
                    
                valid_coactors[coactor].update(genres)

        # Filter co-actors who have acted in at least seven different genres
        valid_coactors = [coactor for coactor, genres in valid_coactors.items() if len(genres) >= 7]

        if not valid_coactors:
            return [("status",), ("error",)]  # No valid co-actors found
        
        # Filter co-actors based on the different genres they have
        starting_actor_genres = set()
        for movie_id in movies:
            query_genres = "SELECT genre_id FROM movie_has_genre WHERE movie_id = %s"
            cur.execute(query_genres, (movie_id,))
            genres = {row[0] for row in cur.fetchall()}
            starting_actor_genres.update(genres)
        
        filtered_coactors = []
        for coactor in valid_coactors:
            # Find genres of movies the coactor acted in, excluding movies with the starting actor
            query_genres = """
                SELECT DISTINCT genre_id
                FROM movie_has_genre
                WHERE movie_id IN (
                    SELECT movie_id
                    FROM role
                    WHERE actor_id = %s
                )
                AND movie_id NOT IN (
                    SELECT movie_id
                    FROM role
                    WHERE actor_id = %s
                )
            """
            cur.execute(query_genres, (coactor, actorId))
            coactor_genres = {row[0] for row in cur.fetchall()}
            if not any(genre in coactor_genres for genre in starting_actor_genres):
                filtered_coactors.append(coactor)

        # Sort co-actors
        filtered_coactors.sort()

        # Prepare the output
        output = [("actorId",)]
        for valid_coactor in filtered_coactors:
            output.append((valid_coactor,))
        
        return output
        
    except Exception as e:
        print(f"An error occurred: {e}")
        return [("status",), ("error",)]
    
    finally:
        cur.close()
        con.close()

def selectTopNactors(n):
    
    # Create a new connection
    con = connection()

    # Create a cursor on the connection
    cur = con.cursor()

    # Convert n to an integer
    n = int(n)

    try:
        result = []

        # Get all genres sorted alphabetically
        query_genres = "SELECT genre_id, genre_name FROM genre ORDER BY genre_name ASC"
        cur.execute(query_genres)
        genres = cur.fetchall()

        for genre_id, genre_name in genres:
            # Find actors for the current genre along with the number of movies they've appeared in
            query_actors = """
                SELECT actor_id, COUNT(*) AS num_movies
                FROM role
                WHERE movie_id IN (
                    SELECT movie_id
                    FROM movie_has_genre
                    WHERE genre_id = %s
                )
                GROUP BY actor_id
            """
            cur.execute(query_actors, (genre_id,))
            actors = cur.fetchall()

            # Now sort by the number of movies in descending order and actor ID in ascending order
            sorted_actors = sorted(actors, key=lambda x: (-x[1], x[0]))

            # Select top N actors for the current genre
            top_actors = sorted_actors[:n]
            for actor_id, num_movies in top_actors:
                result.append((genre_name, actor_id, num_movies))

        # Sort the result alphabetically by genre name, then by the number of movies in descending order,
        # and finally by actor ID in ascending order
        result.sort(key=lambda x: (x[0], -x[2], x[1]))

        return [("genreName", "actorId", "numberOfMovies"),] + result

    except Exception as e:
        print(f"An error occurred: {e}")
        return [("status",), ("error",)]

    finally:
        cur.close()
        con.close()

def traceActorInfluence(actorId):

    # Create a new connection
    con = connection()

    # Create a cursor on the connection
    cur = con.cursor()

    try:
        influenced_actors = set()
        to_process = set()
        influenced_genres = {}

        # Initial processing for the starting actor
        query_initial_movies = """
            SELECT movie_id, year
            FROM movie
            WHERE movie_id IN (
                SELECT movie_id
                FROM role
                WHERE actor_id = %s
            )
        """
        cur.execute(query_initial_movies, (actorId,))
        initial_actor_movies = cur.fetchall()

        for movie_id, year in initial_actor_movies:
            query_genres = "SELECT genre_id FROM movie_has_genre WHERE movie_id = %s"
            cur.execute(query_genres, (movie_id,))
            genres = [row[0] for row in cur.fetchall()]

            query_coactors = "SELECT actor_id FROM role WHERE movie_id = %s AND actor_id != %s"
            cur.execute(query_coactors, (movie_id, actorId))
            coactors = [row[0] for row in cur.fetchall()]

            for coactor in coactors:
                for genre_id in genres:
                    query_later_movies = """
                        SELECT movie_id
                        FROM movie
                        WHERE year > %s AND movie_id IN (
                            SELECT movie_id
                            FROM movie_has_genre
                            WHERE genre_id = %s
                        )
                    """
                    cur.execute(query_later_movies, (year, genre_id))
                    later_movies = [row[0] for row in cur.fetchall()]

                    if later_movies:
                        query_actor_in_later_movie = """
                            SELECT actor_id
                            FROM role
                            WHERE movie_id IN %s AND actor_id = %s
                        """
                        cur.execute(query_actor_in_later_movie, (tuple(later_movies), coactor))
                        if cur.fetchone():
                            if coactor not in influenced_genres:
                                influenced_genres[coactor] = set()
                            influenced_genres[coactor].add(genre_id)
                            to_process.add(coactor)

        # Recursive processing for co-actors
        while to_process:
            current_actor = to_process.pop()
            influenced_actors.add(current_actor)

            if current_actor not in influenced_genres:
                continue

            current_actor_genres = influenced_genres[current_actor]

            for genre_id in current_actor_genres:
                # Get movies of the current actor in the influenced genre
                query_movies = """
                    SELECT movie_id, year
                    FROM movie
                    WHERE movie_id IN (
                        SELECT movie_id
                        FROM role
                        WHERE actor_id = %s
                    ) AND movie_id IN (
                        SELECT movie_id
                        FROM movie_has_genre
                        WHERE genre_id = %s
                    )
                """
                cur.execute(query_movies, (current_actor, genre_id))
                movies = cur.fetchall()

                for movie_id, movie_year in movies:
                    query_coactors = "SELECT actor_id FROM role WHERE movie_id = %s AND actor_id != %s"
                    cur.execute(query_coactors, (movie_id, current_actor))
                    coactors = [row[0] for row in cur.fetchall()]

                    for coactor in coactors:
                        query_later_movies = """
                            SELECT movie_id
                            FROM movie
                            WHERE year > %s AND movie_id IN (
                                SELECT movie_id
                                FROM movie_has_genre
                                WHERE genre_id = %s
                            )
                        """
                        cur.execute(query_later_movies, (movie_year, genre_id))
                        later_movies = [row[0] for row in cur.fetchall()]

                        if later_movies:
                            query_actor_in_later_movie = """
                                SELECT actor_id
                                FROM role
                                WHERE movie_id IN %s AND actor_id = %s
                            """
                            cur.execute(query_actor_in_later_movie, (tuple(later_movies), coactor))
                            if cur.fetchone():
                                if coactor not in influenced_genres:
                                    influenced_genres[coactor] = set()
                                influenced_genres[coactor].add(genre_id)
                                if coactor not in influenced_actors:
                                    to_process.add(coactor)

        # Remove the initial actorId from the result set
        influenced_actors.discard(actorId)

        # Sort the influenced actors
        sorted_influenced_actors = sorted(influenced_actors)

        # Prepare the output
        result = [("influencedActorId",)] + [(actor,) for actor in sorted_influenced_actors]

        return result

    except Exception as e:
        print(f"An error occurred: {e}")
        return [("status",), ("error",)]

    finally:
        cur.close()
        con.close()

