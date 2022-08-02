--找出男性评分最高且评分次数超过 50 次的 10 部电影，展示电影名，平均影评分和评分次数。

SELECT m.movieName, j.avgrate, j.total
FROM 
    (SELECT r.movieId, AVG(r.rate) As avgrate ,count(*) As total
    FROM
        moonxyue.ratings r
    WHERE r.userId IN (
        SELECT u.userId 
        FROM moonxyue.users u
        WHERE u.sex = 'M'
    )
    GROUP BY r.movieId
    HAVING total > 50
    ) j
JOIN moonxyue.movies m
ON m.movieId = j.movieId
Order By j.avgrate DESC
limit 10
