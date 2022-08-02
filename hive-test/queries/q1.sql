--展示电影 ID 为 2116 这部电影各年龄段的平均影评分
SELECT 
    age, AVG(rate) 
FROM
    (SELECT 
        userId, movieId,rate
    FROM moonxyue.ratings
        Where movieId = 2116
    ) As r
    JOIN moonxyue.users u
        ON r.userId = u.userId   
GROUP BY u.age 
Order By u.age
