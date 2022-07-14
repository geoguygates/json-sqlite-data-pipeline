Design:
------
-I used a functional core, imperative shell design to break out the functions from the main top-level program code.

-The first top-level code file is called "extract_main.py" and implements the simple logic to extract the data from it's 
 source formats (json and sqlite) and perform the necessary data wrangling to reformat and standardize the data as four seperate pandas dataframes.
 The four pandas dataframes are then saved as parquet files to a folder called "extract_bucket".
 This step modularizes the pipeline and allows the developer to refactor and/or troubleshoot errors in the codebase more efficiently
 before starting the transformation and load steps of the ETL process.

-The second top-level code file is called "transform_load_main.py" in this step the code applies the necessary bussiness logic including joins and aggregation functions
 to create two of the three final analytics tables requested (transform) in the case study prompt (didn't get to the third)
 The two data tables are then loaded into a sqlite DB called "final_analytics.db". I intentionally chose code the logic so new data would be
 appended onto the already existing tables in "final_analytics.db" each time the pipeline ran so going forward we would have a historic log we could reference if
 the pipeline displayed irregular or incorrect behavior.


Improvements:
---------------
-Add logging.
-Write data tests and implement them in the pipeline.
-think about how CI/CD fit in?

#json
Use a context manager outside of the loop.
Loop through the JSON files one at a time
Use a context manager within the loop top open/close each json file
Use a generator function within a nested loop to process batches of 100 lines of json at a time and write 

for file in files:
	with open(2k3f7s6l4l;8.json)


#sqlite
Use context manager outside of the forloop to open the sqlite connection
then loop through DB 100 lines at a time
	




### References

#Iterating over files
https://stackoverflow.com/questions/24312123/memory-efficent-way-to-iterate-over-part-of-a-large-file
https://stackoverflow.com/questions/61241437/looping-before-or-after-context-manager-in-python


#Generators
https://medium.com/programming-for-beginners/how-to-use-python-generators-to-efficiently-process-large-data-sets-38e18bd8ae29
https://stackoverflow.com/questions/44663040/read-and-print-from-a-text-file-n-lines-at-a-time-using-a-generator-only

#Dask
https://stackoverflow.com/questions/37756991/best-way-to-join-two-large-datasets-in-pandas

#Streaming JSON Data
https://stackoverflow.com/questions/10382253/reading-rather-large-json-files

#Memory usage
https://stackoverflow.com/questions/938733/total-memory-used-by-python-process
