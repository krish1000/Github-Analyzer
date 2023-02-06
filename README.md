# Github-Analyzer
Big Data Project - Utilizes PySpark (RDD), Flask, Docker 

Developed a solution for analyzing GitHub public repository commit data using Python, Apache-Spark, SQL(RDD), Flask, Docker, and HTML & CSS.
A python script does batch runs every 60 seconds on githubs' API repository which retrieves and stream data over TCP to a Spark cluster.
Implemented a Map Reduce algorithm to process and summarize data, and used Flask to present the results in a web-based interface which is run on a Flask server.

Key findings that are presented are the total number of repositories associated with e.g Java vs Python vs C, 
number of collected repositories with changes pushed within the last 60 seconds,
average number of stars found in each repository (grouped by their respective programming language),
and the top 10 most frequent words found in the title/description of the repository.
