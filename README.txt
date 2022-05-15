Design:
------
-I used a functional core, imperative shell design to break out the functions from the main top-level program code.

-The first top-level code file is called "extract_main.py" and implements the simple logic to extract the data from it's 
 source formats (json and sqlite) and perform the necessary data wrangling to reformat and standardize the data as four seperate pandas dataframes.
 The four pandas dataframes are then saved as parquet files to a folder called "extract_bucket".
 This step modularizes the pipeline and allows the developer to refactor and/or troubleshoot errors in the codebase more efficiently
 before starting the transformation and load steps of the ETL process.



Possible Flaws:
---------------
-I need to write unit tests to ensure code quality.
-Talk about design choice to deploy the code to run at 8:30AM, would need to know more about the bussiness case to make this decision.



Future Work
------------
-Ideally a pipeline like this would be deployed as a modular, fault tolerant, cloud architecture composed of microservices
 that could be more easily monitored and automated.


