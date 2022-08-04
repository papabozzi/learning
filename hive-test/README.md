# hive test
This hive-test has queries that are related to the questions with link
[hive queries practices](https://u.geekbang.org/lesson/374?article=542681&utm_source=time_web&utm_medium=menu&utm_term=timewebmenu).
=======================
### Original table description
 - ratings
   - schema
     >userid :          	int                 	   
      movieid:             	smallint            	  
      rate   :             	tinyint             	   
      ts     :            	int      
   - sample data

     | ratings.userid | ratings.movieid | ratings.rate | ratings.ts |
     |----------------|-----------------|--------------|------------|
     | 1              | 1193            | 5            | 978300760  |
     | 1              | 661             | 3            | 978302109  |
     | 1              | 914             | 3            | 978301968  |
     | 1              | 3408            | 4            | 978300275  |
     | 1              | 2355            | 5            | 978824291  |
 - movies
   - schema
     >movieid :            	smallint   
      moviename :           string    
      movietype :          	string 
   - sample data
   
     | movies.movieid | movies.moviename                   | movies.movietype              |
     |----------------|-----------------------------------|--------------------------------|
     | 1              | Toy Story (1995)                   | Animation\|Children's\|Comedy  |
     | 2              | Jumanji (1995)                     | Adventure\|Children's\|Fantasy |
     | 3              | Grumpier Old Men (1995)            | Comedy\|Romance                |
     | 4              | Waiting to Exhale (1995)           | Comedy\|Drama                  |
     | 5              | Father of the Bride Part II (1995) | Comedy                        |

 - users
   - schema
     > userid    :          	int                 	   
       sex       :            	char(1)             	   
       age       :           	tinyint             	   
       occupation:          	smallint            	   
       zipcode   :          	char(5)             	
   - sample data

     | users.userid | users.sex | users.age | users.occupation | users.zipcode |
     |--------------|-----------|-----------|------------------|---------------|
     | 1            | F         | 1         | 10               | 48067         |
     | 2            | M         | 56        | 16               | 70072         |
     | 3            | M         | 25        | 15               | 55117         |
     | 4            | M         | 45        | 7                | 02460         |
     | 5            | M         | 25        | 20               | 55455         |

## Script Explanation:

### prepare.sql 

hive-test/queries/prepare.sql is used to create table and load
data into hive.

### q1.sql :

- <i>hive-test/queries/q1.sql: the query of the first question<br>
- output

| age | avgrate            |
|-----|--------------------|
| 1   | 3.2941176470588234 |
| 18  | 3.3580246913580245 |
| 25  | 3.436548223350254  |
| 35  | 3.2278481012658227 |
| 45  | 2.8275862068965516 |
| 50  | 3.32               |
| 56  | 3.5                |

### q2.sql:

- <i>hive-test/queries/q2.sql: the query of the second question<br>
- output

| movieName                                                           | avgrate            | total |
|---------------------------------------------------------------------|--------------------|-------|
| Sanjuro (1962)                                                      | 4.639344262295082  | 61    |
| Godfather, The (1972)                                               | 4.583333333333333  | 	1740 |
| Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954) | 4.576628352490421  | 522   |
| Shawshank Redemption, The (1994)                                    | 4.560625 	         | 1600  |
| Raiders of the Lost Ark (1981)                                      | 4.520597322348094  | 	1942 |
| Usual Suspects, The (1995)                                          | 4.518248175182482	 | 1370  |
| Star Wars: Episode IV - A New Hope (1977)	                          | 4.495307167235495  | 2344  |
| Schindler's List (1993)                                             | 4.49141503848431   | 1689  |
| Paths of Glory (1957)                                               | 4.485148514851486  | 	202  |
| Wrong Trousers, The (1993)                                          | 4.478260869565218  | 	644  |

### q3.sql:
- <i>hive-test/queries/q3.sql: the query of the third question<br>
- output

| moviename                                                                   | avgrate            |
|-----------------------------------------------------------------------------|--------------------|
| Close Shave, A (1995)                                                       | 4.52054794520548   |
| Rear Window (1954)                                                          | 4.476190476190476  |
| Dr. Strangelove or: How I Learned to Stop Worrying and Love the Bomb (1963) | 4.4498902706656915 |
| It Happened One Night (1934)                                                | 4.280748663101604  |
| Duck Soup (1933)                                                            | 4.21043771043771   |
| Trust (1990)                                                                | 4.188888888888889  |
| Being John Malkovich (1999)                                                 | 4.125390450691656  |
| Roger & Me (1989)                                                           | 4.0739348370927315 |
| Night on Earth (1991)                                                       | 3.747422680412371  |
| Crying Game, The (1992)                                                     | 3.7314890154597236 |