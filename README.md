# Homework 7: Data Integration

- Loading Data (10)
- Cleaning Data (30)
  - String similarity (3 fields)
  - Blocking (record and string)
- Uploading Data (25)
  - Selecting master business record 
- Joining Data (10)
- Writeup (15)

The goal of this assignment is to integrate and clean a 'dirty' dataset. You will be integrating the SF bike trip information with restaurant inspection data. 

At the end of the two part assignment, you will have uploaded the SF inspection data into your Postgres database table `sfinspection` and transformed the raw 'dirty' data into two tables with fields as defined in `schemas.sql`:
1. `cleanrest` will contain your authoritative restaurant details
2. `cleaninspection` will link inspections to the cleaned restaurants

In the last part of this assignment, you will join your clean data with bike data from HW6 into a table called `joinedinspbike`. 

You will also have a chance to explain any design choices in a final writeup.

## Running the code
- You should run `python3 insdriver.py` to run all phases of the data loading, cleaning and integration. Run `python3 insdriver.py --help` to see the arguments.
- You should only modify `rest_inspection.py` and `connection.py` for the homework.


## Notes
- Several libraries have been pre-installed on the VMs including fuzzywuzzy, python-Levenshtein, jellyfish (for string matching)
- The schema for the required tables are fixed, but you can create temporary/intermediate tables for cleaning/blocking/etc. 
- You will likely need to do a mix of python and SQL code for the cleaning. You might need some python for blocking. **Please do not use Pandas for any part of this assignment. You will receive zero credit.**
- Use the same database as your HW6. You will want to make sure that you have completed HW6 and properly loaded the 2017 Ford Go Bike data into your database. We will be using those tables for the join phase (Step 3)of the assignment. 
- You should be able to connect and use your database from anywhere on campus/vpn. So you can run python from your laptop and use the DB in the VM. In the end you need to have everything running on the VM.


## Step 0: Loading the file. (10 pts)

You will modify the `load_inspection` function in the `rest_inspection.py` file. The uncleaned (raw) inspection data should be loaded into a table `sfinspection` using the provided schema (`schemas.sql`).
Load inspection data via provided TSV.

## Step 1: Cleaning the records.

You will notice that the raw inspection table contains denormalized data containing business and inspection information. Moreover, the restaurant data also contains many errors, with the same restaurant being expressed in different names, abbreviations, addresses, etc. We will want all inspections for the same business to be mapped to a single restaurant record.

Consider the following rows as an example:
```
 business_name, business_address, inspection_id
 Aarons Emporium Store, 123 Main St., 100
 Aarons Emporium Store, 123 Min St., 101
 A A Rons Emporium Store, 123 Main St., 104
 Aarons Store, 123 Main St., 105
 Starbucks, 123 Main St., 110
```
Clearly, the first 4 entries should be linked with the same restaurant. Clean the raw data using the following strategies. 

  
### 1.1 Blocking (10 pts)
Implement a method `find_cands` that implements a blocking solution that finds a set of possible "promising matches." Use one record-level blocking, and one string blocking strategy. 
- For record-level blocking, you will want to find a strategy that reduces the number of string similarity comparisons you make. 
- Consider using cheap string filters to eliminate non-matches.
- You may want to consider using indexing for your blocking strategy. Take a look at all of psql's indexes [https://www.postgresql.org/docs/9.5/static/indexes-types.html](https://www.postgresql.org/docs/9.5/static/indexes-types.html)

### 1.2 Computing Similarity Scores (20pts)
Implement a method `compute_similarity` that computes string similarity for business name and your choice of two additional fields for every pair of records identified in your candidate set. *Do not use lat and long for this comparison*
- Using at least one edit distance based algorithm and one set based algorithm, compute similarity scores for each of your chosen fields;
- Several libraries have been pre-installed on the VMs including fuzzywuzzy, python-Levenshtein, and jellyfish, and you are free to get creative with any pre-processing steps you choose; justify your choices in your writeup.
- You are free to create any intermediate/temp tables for your cleaning and use the built-in psycopg2 iterators, but again please do not use Pandas.

## Step 2: Integrating the Data

Using the blocking and string matching functions defined in Step 1, transform the `sfinspection` table (raw inspections data) into two tables with fields as defined in `schemas.sql`:
1. `cleanrest` will contain your authoritative restaurant details
2. `cleaninspection` will link inspections to the cleaned restaurants

### 2.1 Select Authoritative Restaurant (15 pts)
Using the similarity scores computed in 1.2, define a method that determines the authoritative restaurant record and other records that match it. 
This record can be a composite of other records (e.g. name from record1, address from record2), or you could define custom rules to make a new record
(e.g. merge names into a new name).
A simple rule-based matching technique is fine for this task.

### 2.2 Populate tables with Authoritative Restaurants and Corresponding Inspections (5 pts)
Implement a method `update_matches` which populates the `cleanrest` and `cleaninspection` tables based on the above cleaning.

### 2.3 Putting it together (5 pts)
Integrate Steps 1 and 2 to perform all cleaning and uploading in a method called `clean_dirty_inspection` in `rest_inspection.py`.


## Step 3: Join Bike Data
### 3.1 Join Inspection with Bike Data (10 pts)
Suppose we want to find bike trips that end within a certain distance of restaurants using a simple measure of distance. Implement a `join_trips` method that accomplishes the following:
- Using the provided distance function, find bike trips ending within one week after a violation at restaurants  (based on the cleaned data) whose location is within 800 meters of the trip destination.
 Checking if lat,long, is within 
 Lat    +/- .00720772
 Long +/- 0.0090810 
(note this is not very accurate way of measuring distance!)
  

- Use the results of the query to populate the table `joinedinspbike` with columns defined in `schemas.sql`

## Step 4: Write Up (15 pts)
Provide a detailed write up, outlining your design decisions, methods evaluated, and how you and your partner split the work. 
