# movielens

The project consists of 4 jobs: 
* StageRatings - to upsert ratings into a delta table after partitioning 
* StageMoviesTags - loads movies and tags and stores them into delta table after cleaning up the columns and casting 
* TransformMovies - exploding the genres column in movies and saving the output as a delta table 
* TransformTopTen - saving top ten movies with the highest avg rating and no of ratings > 10 into a single csv file 



### Steps to run:

The repo contains a build file which can be leveraged to create a venv and run the code without any hassles:

* Running `make all` will run all the commands in correct order of requirement.   
* The make file creates a .venv using the default python and installs all the dependencies inside the same.   
* It runs a few test cases and then creates an egg file with the code packaged inside it.   
* Finally, it activates the .venv that was created so that the spark application can run in it.   

***Run Command***  
 
```spark-submit --packages io.delta:delta-core_2.12:1.1.0 --py-files asos-movielens/dist/asos_movielens-0.0.1-py3.10.egg main.py --job-name transformation.transform_topten --class-name TransformTopTen  --file-loc ~/ml-latest-small/ --sink-loc ~/ml-latest-small```

Arguments: 
* `--job-name`: name of the module which needs to be run: 
    *  staging.stage_reviews, 
    *  staging.stage_movies_tags, 
    *  transformation.transform_movies, 
    *  transformation.transform_topten  
* `--class-name`: name of the class inside the module which contains the compute function: 
    *  staging.stage_reviews -> StageRatings
    *  staging.stage_movies_tags -> StageMoviesTags
    *  transformation.transform_movies -> TransformMovies
    *  transformation.transform_topten -> TransformTopTen
* `--file-loc`: absolute path to the directory of the movie lens csvs
* `--sink-loc`: path where the data should be saved 

The main file contains the driver code and needs the egg file if it's not being run from root. 
