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
        LIMIT 1;

CREATE TEMPORARY TABLE movie_10 As
    SELECT r.movieId
    FROM moonxyue.ratings r
    WHERE r.userId IN (
        SELECT userId
        FROM female
    )
    ORDER BY rate DESC
    LIMIT 10;

SELECT movieName, avgrate
FROM
    (SELECT movieId, AVG(rate) As avgrate
            FROM moonxyue.ratings r
            WHERE r.movieId IN (
                SELECT movieId
                FROM movie_10
            )
GROUP BY movieId) As m_r
JOIN moonxyue.movies m
ON m.movieId = m_r.movieId
ORDER BY avgrate DESC