# Project Description

In the completion of this project, I focussed on abstracting specific tasks
as individual methods. I also decided to add an attribute to the client class
to store an open connection to preserve this connection as different methods
are applied. Below is a summary of the purpose of each method (excluding
open_connection and close_connection) and specific decisions made at each step.

## Running Code
Run python3 insdriver.py to run all phases of the data loading, cleaning and integration. Run python3 insdriver.py --help to see the arguments.

## Functions

build_tables
The build_tables method creates all necessary tables for the entire analysis
using the specified schemas. This includes the sfinspection table,
cleanrest table, cleaninspection table, and trips table (in anticipation
of Step 3). A cursor object is generated and used to execute each CREATE TABLE
query. Before each query is run, I make sure to drop the table if it already
exists, to avoid any conflicts.

load_inspection
The load_inspection method bulk loads the raw inspection data via TSV to the
sfinspection table. I leverage my bulk load code from HW6 to accomplish this
task, which involves copying the raw data into a temporary table (copy_expert),
and finally using an INSERT INTO statement to upload the necessary fields.
I also decided to add a unique ID to each row which I use throughout the work
flow to keep track of which inspection I am referring to (ie. if a business
name changes during cleaning, this allows me to keep track of the original
identity of the record). This unique_id also allows me to understand
when a record is being joined on itself (later on), which is important when
determining which records are truly matches and which are simply duplicates.

load_trips
The load_trips method loads the trip data for Step 3 of the assignment. It uses
the same bulk loading method described above, and I have decided to hard-code
in the 'trip.csv' file which I manually import from HW6 to use at the end
of the assignment.

build_indexes
The build_indexes method creates a generalized inverted index (gin) on
sfinspection, specifically on the first two letters of the business name
field. This helps make the second level of my blocking strategy more efficient
and speeds up the full text search for rows of data with a business_name that
share the same first two letters within a given zip code. Gin indexes are not
lossy for standard queries, and the documentation states that gin indexes are
about three times faster than gist, and are appropriate for static data
because lookups are faster. In this case, the data are static.

determine_zips
The determine_zips method returns a list of distinct zip codes in the full,
raw dataset. This method is called during record-level blocking. My blocking
solution finds a set of possible "promising matches" and reduces the number
of string similarity comparisons by grouping together inspections within
a single zip code and later by first two letters of business name.
This occurs in clean_dirty_data.

compute_similarity, jaccard, and edit_distance
The compute_similarity method generates a final similarity score between
two records that have been joined together in a temp table as a cross
product. Specifically, it computes the similarity between the cleaned
business names of the two records using the jaccard method. Jaccard is a
set-based metric, and for the purposes of this calculation a token is
generated from each word in the business name. I use this metric because I am
specifically interested in capturing business names that have a majority of
the same words in the title. Compute_similarity also calculates the similarity
between the cleaned addresses of the two records using edit_distance
(Levenshtein distance), which computes the minimal cost of transforming
x into y and converts this into a similarity measure. The same method is used
to compare phone numbers. I decided to use distance-based metrics for these
fields because the specific positioning and ordering of characters are very
important. For instance, while two phone numbers with a single switched
digit would have no tokens in common, they are in fact very similar and likely
matches, which would be better captured with an edit distance measure.
The same goes for addresses. These scores are calculated AFTER the records have
been pre-processed, as discussed below. For the final score, business_name
is most heavily weighted (.6), followed by address (.3), and phone_number (.1),
since several records are missing phone numbers all together.

determine_match_non_match
The determine_match_non_match method determines if two clean records are
matches using a score threshold of .75, which I determined based on assessing
the distribution of scores. Note that the first level of blocking is by zip-code,
and the second level of blocking roups grecords with the same first
two letters of business name AFTER pre-processing. For each block,
if records are are match, the unique_id of each record is added to a set called
match_ids. If two records are not a match, their unique_ids are added to a set
called non_match_ids. At the end of the method, match_ids are subtracted out
of non_match_ids to ensure that no matches make it into the non_match
group. It is important to note that before calling this function, there is a
a check that states that similarity scores will not be computed when a
record is joined on itself (unique_id = unique_id), to avoid unnecessary
computation cost.

update_matches
This method is called "block-by-block" in clean_dirty_inspection, and
is used to populate the cleanrest and cleaninspection tables based on the
blocking/cleaning strategy.

For each non-match set, I iterate through the unique_ids and for each one,
I pull the clean business information and inspection information directly into
the final tables (along with all other fields from the original row of data).
For each match-set, I use the following rules to determine a single,
authoritative restaurant record to include in the clean restaurant table:

    1. If there are multiple linked business names, select the most
       frequent one in the original dataset. If there is a tie between two
       business names, select the one that has the longest name. If
       multiple have the same length name, select the one that appears
       first.
    2. Same rule as above for multiple linked addresses.
    3. If there are multiple phone numbers, select the most frequent one
       in the original dataset. If there is a tie, select one at random.
    4. For all other fields, selection made at random, based on the first
       entry in the match-set.

I use the logic that longer business names are likely to be more precise
in the case of a tie between business names that occur frequently.
I use the logic that longer addresses are likely to spell out words like
"street" and "avenue", and therefore I consider these authoritative.
After I load an authoritative record (created from components of matched
records) into the cleanrest table, I then iterate through the match_ids,
and add all associated clean inspection records to cleaninspection using
the cleaned, authoritative business name and address.

clean_dirty_inspection
The clean_dirty_inspection method performs all cleaning and uploading in a
fluid, block-by-block flow. Note that this function also performs the role
of implementing a blocking solution to determine a set of promising candidate
matches (find_cands), since cleaning and uploading occurs one "block" at a
time. The following is the order of opeations used by this method:

    1. Cleans records
    2. Determine potential matches (blocking solution)
    	-blocks by zip-code
    	-blocks businesses that share the same first two letters in their names
    3. Computes similarity scores
    	-weighted, based on business name, address, and phone number
    4. Determine matches and non-matches
    5. Creates authoritative business records
    6. Updates authoritative restaurant and inspection tables accordingly

In this method, cleaning takes place before similarity scores are computed.
Specifically, I leverage the following cleaning techniques:

1. business name: convert all characters to lowercase, remove all characters
that are not letters, numbers, or spaces (since it is very common for
some records to have punctutation in some cases and not in others), and
remove stop words such as and, or, if, and the. I noticed that 'The' is important
to remove since many very different businesses begin with the word 'The' but
have nothing in common.
2. address: convert all characters to lowercase and remove all characters
that are not letters, numbers, or spaces (since it is very common for
some addresses to have punctutation and not others). Stop words are less of an
issue when it comes to addresses.
3. phone number: remove + from the beginning of phone numbers, when included,
for consistency, since all phone numbers should be in the United States
(same country code)

join_tables
Finally, the join_tables method completes Step 3 of the assignment. It joins
cleaned restaurants, inspections, and bike rides together (using the trips.csv
file from HW6), and identifies bike trips ending within one week after a
violation at restaurants whose location is within 800 meters of the trip destination.
I leverage a nested query that:
1. Identifies rows in the inspection data that have data for violation_id
(indicating that there was a violation), as well as latitude and longitude data,
and from these records pulls out the corresponding business's latitude and
longitude from the cleanrest table.
2. Using this group of violations, creates a join with trips where records
are only kept if the trip-end is within the correct radius of the violation
during the specified one-week window.

Once the appropriate records are identified, the data are inserted into the
joinedinspbike table.

In the end, I run the full pipeline using the file Sf-Rest-10per.tsv,
which results in 4396 clean inspections and 2319 cleaned restuarants.
"""
