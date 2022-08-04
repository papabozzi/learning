--找出影评次数最多的女士所给出最高分的 10 部电影的平均影评分，展示电影名和平均影评分

CREATE
    TEMPORARY TABLE female AS
    SELECT userId, COLLECT_SET(movieId) As s_movie, count(*) As total
        FROM
            moonxyue.ratings r
        WHERE r.userId IN (
            SELECT u.userId
            FROM moonxyue.users u
            WHERE u.sex = 'F'
        )
        GROUP BY r.userId
        ORDER BY total DESC
        LIMIT 1

CREATE TEMPORARY TABLE movie_10 As
    SELECT r.movieId, AVG(rate) As avgrate
    FROM moonxyue.ratings r
    WHERE r.movieId IN (
        SELECT EXPLODE(s_movie)
        FROM female
    )
    GROUP BY movieId
    ORDER BY avgrate DESC
    LIMIT 10

SELECT m.movieName, movie_10.avgrate
FROM (
    moonxyue.movies m JOIN movie_10
    ON m.movieId = movie_10.movieId
)





SELECT m.movieName, j.avgrate
FROM

JOIN moonxyue.movies m
ON m.movieId = j.movieId
Order By j.avgrate DESC