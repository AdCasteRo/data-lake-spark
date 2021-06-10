# DATA LAKE

The project "Data Lake" consists on a single python script (etl.py) and one config archive (dl.cfg). In order to debug the project, a Jupyter Notebook has been added but it is not needed to execute the code in any way.

## Purpose of the data lake

The purpose of this project is to get the information from Udacity's data lake ("s3a://udacity-dend/"), load all the JSONs in a Spark session, process them and store them in a designate route. The output is structured in 5 tables explaned in the next point.

## Analytical database justification

The analytical database has a star schema and is formed by one fact table (songplays) and 4 dimension tables (users, songs, artists and time).

The information of songs and artists is taken from the JSON files stored into song_data. Songplays, users and time is taken from the log files stored into log_data.

## Use of the project

- Copy your credentials into "dwh.cfg".
- Execute "etl.py".

## Sources

This project includes code provided by Udacity for the creation of this project.

This project includes code form previous exercises.

