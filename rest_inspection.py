from hdrh import histogram
import sys, traceback
import argparse
import logging
from datetime import datetime
import connection
import psycopg2 as pg
from math import*
from fuzzywuzzy import fuzz


#logging
logger = logging.getLogger('sfinspect')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
logger.addHandler(ch)


class client:
    def __init__(self):
        self.dbname = connection.dbname
        self.dbhost = connection.dbhost
        self.dbport = connection.dbport
        self.dbusername = connection.dbusername
        self.dbpasswd = connection.dbpasswd
        self.conn = None


    def open_connection(self):
        '''
        Opens a connection to a PSQL database, using the self.dbXX parameters.
        '''
        logger.debug("Opening a Connection")
        conn = pg.connect(dbname=self.dbname, user=self.dbusername,
                          host=self.dbhost, port=self.dbport)
        conn.autocommit = False
        self.conn = conn

        return True


    def close_connection(self):
        '''
        Closes any active connection (able to handle closing a closed conn).
        '''
        logger.debug("Closing Connection")
        self.conn.close()

        return True


    def build_tables(self):
        '''
        Creates all necessary tables.
        '''
        logger.debug("Building Tables")
        drop_inspection = """DROP TABLE IF EXISTS sfinspection CASCADE;"""
        sfinspection = """CREATE TABLE sfinspection (
                              business_name             varchar(100)    ,
                              business_address          varchar(100)    ,
                              business_city             varchar(100)    ,
                              business_state            varchar(100)    ,
                              business_postal_code      varchar(100)    ,
                              business_latitude         float8   ,
                              business_longitude        float8    ,
                              business_phone_number     varchar(20)    ,
                              inspection_id             varchar(100)    ,
                              inspection_date           date    ,
                              inspection_score          real,
                              inspection_type           varchar(100)    ,
                              violation_id              varchar(100)    ,
                              violation_description     varchar(100)    ,
                              risk_category             varchar(100)    );"""
        drop_cleanrest = """DROP TABLE IF EXISTS cleanrest CASCADE;"""
        cleanrest = """CREATE TABLE cleanrest (
                           business_name             varchar(100)    ,
                           business_address          varchar(100)    ,
                           business_city             varchar(100)    ,
                           business_state            varchar(100)    ,
                           business_postal_code      varchar(100)    ,
                           business_latitude         float8   ,
                           business_longitude        float8    ,
                           business_phone_number     varchar(20) ,
                           primary key (business_name,business_address));"""
        drop_cleaninspection = """DROP TABLE IF EXISTS cleaninspection CASCADE;"""
        cleaninspection = """CREATE TABLE cleaninspection (
                                 business_name             varchar(100)    ,
                                 business_address          varchar(100)    ,
                                 inspection_id             varchar(100)    ,
                                 inspection_date           date    ,
                                 inspection_score          real ,
                                 inspection_type           varchar(100)    ,
                                 violation_id              varchar(100)    ,
                                 violation_description     varchar(100)    ,
                                 risk_category             varchar(100)   ,
                                 foreign key (business_name,business_address)
                                 references cleanrest);"""
        drop_trips = """DROP TABLE IF EXISTS trips;"""
        trips = """CREATE TABLE trips (
                       trip_id serial PRIMARY KEY,
                       duration_seconds int DEFAULT NULL,
                       start_time timestamp DEFAULT NULL,
                       end_time timestamp DEFAULT NULL,
                       bike_id int DEFAULT NULL,
                       user_type varchar(10) DEFAULT NULL,
                       member_birth_year int DEFAULT NULL,
                       member_gender varchar(6) DEFAULT NULL,
                       start_station_id int,
                       end_station_id int,
                       end_station_latitude float(53),
                       end_station_longitude float(53));"""
        drop_joined = """DROP TABLE IF EXISTS joinedinspbike;"""
        joinedinspbike = """CREATE TABLE joinedinspbike (
                                duration int,
                                bike_id int,
                                violation_id varchar(100),
                                inspection_date date);"""

        cur = self.conn.cursor()
        cur.execute(drop_inspection)
        cur.execute(sfinspection)
        cur.execute(drop_cleanrest)
        cur.execute(cleanrest)
        cur.execute(drop_cleaninspection)
        cur.execute(cleaninspection)
        cur.execute(drop_trips)
        cur.execute(trips)
        cur.execute(drop_joined)
        cur.execute(joinedinspbike)
        self.conn.commit()
        cur.close()

        return True


    def load_inspection(self, limit_load=None, load_file=None):
        '''
        Bulk loads the raw inspection data via TSV to the sfinspection table
        and creates a unique_id (serial) for each record.
        '''
        logger.debug("Loading Inspection")
        if load_file == None:
            raise Exception("No Load Details Provided")

        drop = """DROP TABLE IF EXISTS temp;"""
        temp_table = """CREATE TEMPORARY TABLE temp (
                            business_name             varchar(100)    ,
                            business_address          varchar(100)    ,
                            business_city             varchar(100)    ,
                            business_state            varchar(100)    ,
                            business_postal_code      varchar(100)    ,
                            business_latitude         float8          ,
                            business_longitude        float8          , 
                            business_phone_number     varchar(20)     ,
                            inspection_id             varchar(100)    ,
                            inspection_date           date            ,
                            inspection_score          real            ,
                            inspection_type           varchar(100)    ,
                            violation_id              varchar(100)    ,
                            violation_description     varchar(100)    ,
                            risk_category             varchar(100)    );"""
        statement = "COPY temp FROM STDIN DELIMITER E'\t' CSV HEADER"
        insert = """INSERT INTO sfinspection (business_name, business_address,
                    business_city, business_state, business_postal_code,
                    business_latitude, business_longitude,
                    business_phone_number, inspection_id, inspection_date,
                    inspection_score, inspection_type, violation_id,
                    violation_description, risk_category)
                    SELECT business_name, business_address,
                    business_city, business_state, business_postal_code,
                    business_latitude, business_longitude,
                    business_phone_number, inspection_id, inspection_date,
                    inspection_score, inspection_type, violation_id,
                    violation_description, risk_category
                    FROM temp
                    ON CONFLICT DO NOTHING"""

        cur = self.conn.cursor()
        cur.execute(drop)
        cur.execute(temp_table)
        with open(load_file) as f:
            cur.copy_expert(statement, f)
        cur.close()

        cur = self.conn.cursor()
        cur.execute(insert)
        cur.execute("""ALTER TABLE sfinspection ADD unique_id SERIAL""")
        cur.close()
        self.conn.commit()

        return True


    def load_trips(self, file_path='trip.csv'):
        '''
        Bulk loads the trip data (using trip.csv file from hw6).
        '''
        logger.debug("Bulk load file from %s" % file_path)

        drop = """DROP TABLE IF EXISTS temp;"""
        temp_table = """CREATE TEMPORARY TABLE temp (
                            duration_seconds int DEFAULT NULL,
                            start_time timestamp DEFAULT NULL,
                            end_time timestamp DEFAULT NULL,
                            start_station_id int DEFAULT NULL,
                            start_station_name varchar(100) DEFAULT NULL,
                            start_station_lat float(53) DEFAULT NULL,
                            start_station_long float(53) DEFAULT NULL,
                            end_station_id int DEFAULT NULL,
                            end_station_name varchar(100) DEFAULT NULL,
                            end_station_lat float(53) DEFAULT NULL,
                            end_station_long float(53) DEFAULT NULL,
                            bike_id int DEFAULT NULL,
                            user_type varchar(10) DEFAULT NULL,
                            member_birth_year int DEFAULT NULL,
                            member_gender varchar(6) DEFAULT NULL);"""
        statement = """COPY temp FROM STDIN DELIMITER ',' CSV HEADER
                       FORCE NULL member_birth_year"""
        insert3 = """INSERT INTO trips (duration_seconds, start_time, end_time,
                     bike_id, user_type, member_birth_year, member_gender,
                     start_station_id, end_station_id, end_station_latitude,
                     end_station_longitude)
                     SELECT duration_seconds, start_time, end_time, bike_id,
                     user_type, member_birth_year, member_gender,
                     start_station_id, end_station_id, end_station_lat,
                     end_station_long
                     FROM temp
                     ON CONFLICT DO NOTHING"""

        cur = self.conn.cursor()
        cur.execute(drop)
        cur.execute(temp_table)
        with open(file_path) as f:
            cur.copy_expert(statement, f)
        cur.close()

        cur = self.conn.cursor()
        cur.execute(insert3)
        cur.close()
        self.conn.commit()

        return True


    def build_indexes(self):
        '''
        Adds an inverted (gin) index to sfinspection.
        '''
        logger.debug("Building Indexes")

        query_1 = """CREATE INDEX inverted
                     ON sfinspection
                     USING gin(to_tsvector('english', LEFT(business_name, 2)));"""
        cur = self.conn.cursor()
        cur.execute(query_1)
        cur.close()
        self.conn.commit()

        return True


    def determine_zips(self):
        '''
        Returns a list of distinct zip codes in the full, raw dataset.
        '''
        query_1 = """SELECT DISTINCT business_postal_code
                     FROM sfinspection"""
        cur = self.conn.cursor()
        cur.execute(query_1)
        zip_codes = cur.fetchall()
        cur.close()

        return zip_codes


    def jaccard(self, string_1, string_2):
        '''
        Calculates a jaccard score for two strings provided as input.
        '''
        if string_1 == None or string_2 == None:
            return 0
        else:
            intersection = len(set.intersection(*[set(string_1), set(string_2)]))
            union = len(set.union(*[set(string_1), set(string_2)]))
            score = intersection / float(union)

        return score


    def edit_distance(self, string_1, string_2):
        '''
        Uses Levenshtein distance to compute the minimal cost of transforming
        x into y and converts this into a similarity measure.
        '''
        if string_1 == None or string_2 == None:
            return 0
        else:
            score = fuzz.ratio(string_1, string_2)

        return score / 100


    def compute_similarity(self, business_A, address_A, phone_A,
                           business_B, address_B, phone_B):
        '''
        Computes string similarity for business name, business address, and
        business phone number for every pair of records in candidate set,
        which are passed in as parameters.
        '''
        score_1 = self.jaccard(business_A, business_B)
        score_2 = self.edit_distance(address_A, address_B)
        score_3 = self.edit_distance(phone_A, phone_B)
        final_score = .6*score_1 + .3*score_2 + .1*score_3

        return final_score


    def determine_match_nonmatch(self, cur2, match_ids, non_match_ids):
        '''
        Given a block of potential matches, outputs a set of unique record
        ids for records that are matches and another set of unique record
        ids for records taht do not have matches, based on a similarity
        score threshold.
        '''
        for record in cur2:
            if record[0] != record[4]: # if unique_ID does not equal unique_ID
                score = self.compute_similarity(record[1], record[2], record[3],
                                                record[5], record[6], record[7])
                if score >= 0.75:
                    match_ids.add(record[0])
                    match_ids.add(record[4])
                else:
                    non_match_ids.add(record[0])
                    non_match_ids.add(record[4])
            else:
                non_match_ids.add(record[0])
        non_match_ids = non_match_ids - match_ids

        return match_ids, non_match_ids


    def update_matches(self, cur, match_ids, non_match_ids):
        '''
        Populates the cleanrest and cleaninspection tables based on the
        blocking/cleaning strategy defined above.

        Authoritative restaurant rules:

        1. If there are multiple linked business names, selects the most
           frequent one in the original dataset. If there is a tie between two
           business names, selects the one that has the longest name. If
           multiple have the same length name, selects the one that appears
           first.
        2. Same rule as above for multiple linked addresses.
        3. If there are multiple phone numbers, selects the most frequent one
           in the original dataset. If there is a tie, selects one at random.
        4. For all other fields, selection made at random, based on the first
           entry in the match set.
        '''
        query_8 = """SELECT
                     regexp_replace(lower(business_name),
                     '[^a-zA-Z0-9\s]+|\y(and\s|or\s|if\s|the\s)\y', '','g')
                        AS business_name
                     , regexp_replace(lower(business_address),
                     '[^a-zA-Z0-9\s]+','','g')
                        AS business_address
                     , business_city
                     , business_state
                     , business_postal_code
                     , business_latitude
                     , business_longitude
                     , trim('+' FROM business_phone_number)
                        AS business_phone_number
                     FROM sfinspection
                     WHERE unique_id = (%s);"""
        query_9 = """INSERT INTO cleanrest VALUES %s
                     ON CONFLICT DO NOTHING RETURNING *;"""
        query_10 = """SELECT
                     regexp_replace(lower(business_name),
                     '[^a-zA-Z0-9\s]+|\y(and\s|or\s|if\s|the\s)\y', '','g')
                        AS business_name
                     , regexp_replace(lower(business_address),
                     '[^a-zA-Z0-9\s]+','','g')
                        AS business_address
                     , inspection_id
                     , inspection_date
                     , inspection_score
                     , inspection_type
                     , violation_id
                     , violation_description
                     , risk_category
                     FROM sfinspection
                     WHERE unique_id = (%s);"""
        query_11 = """INSERT INTO cleaninspection VALUES %s
                      ON CONFLICT DO NOTHING RETURNING *;"""
        query_12 = """SELECT
                     business_city
                     , business_state
                     , business_postal_code
                     , business_latitude
                     , business_longitude
                     FROM sfinspection
                     WHERE unique_id = (%s);"""
        query_13 = """SELECT
                     inspection_id
                     , inspection_date
                     , inspection_score
                     , inspection_type
                     , violation_id
                     , violation_description
                     , risk_category
                     FROM sfinspection
                     WHERE unique_id = (%s);"""

        # if a record has no potential matches, upload cleaned data from
        # record to cleanrest and cleaninspection
        if non_match_ids:
            for non_match in non_match_ids:
                cur.execute(query_8, (non_match,))
                record = cur.fetchone()
                cur.execute(query_9, (record,))
                cur.execute(query_10, (non_match,))
                record = cur.fetchone()
                cur.execute(query_11, (record,))
                self.conn.commit()

        # if a record has matches, use rules above to determine
        # authoritative properties; load a single record to cleanrest
        businesses = {}
        addresses = {}
        phones = []
        if match_ids:
            for match in match_ids:
                cur.execute(query_8, (match,))
                record = cur.fetchone()
                businesses[record[0]] = businesses.get(record[0], 0) + 1
                addresses[record[1]] = addresses.get(record[1], 0) + 1
                phones.append(record[7])
            frequent_business = [key for m in [max(businesses.values())] for\
                                 key, val in businesses.items() if val == m]
            frequent_address = [key for m in [max(addresses.values())] for\
                                key, val in addresses.items() if val == m]
            if len(frequent_business) == 1:
                authoritative_business_name = frequent_business[0]
            else:
                authoritative_business_name = max(frequent_business, key=len)
            if len(frequent_address) == 1:
                authoritative_business_address = frequent_address[0]
            else:
                authoritative_business_address = max(frequent_address, key=len)
            if len(phones) == 1:
                authoritative_business_phone_number = phones[0]
            else:
                authoritative_business_phone_number = max(set(phones),
                                                          key=phones.count)
            cur.execute(query_12, (next(iter(match_ids)),))
            record = cur.fetchone()
            authoritative_business_city = record[0]
            authoritative_business_state = record[1]
            authoritative_business_postal_code = record[2]
            authoritative_business_latitude = record[3]
            authoritative_business_longitude = record[4]
            values_r = (authoritative_business_name,
                        authoritative_business_address,
                        authoritative_business_city,
                        authoritative_business_state,
                        authoritative_business_postal_code,
                        authoritative_business_latitude,
                        authoritative_business_longitude,
                        authoritative_business_phone_number)
            cur.execute(query_9, (values_r,))
            self.conn.commit()

            # if a record has matches, adds all associated clean inspection
            # records to cleaninspection (w/ authoritative business name and
            # address)
            for match in match_ids:
                cur.execute(query_13, (match,))
                record = cur.fetchone()
                authoritative_inspection_id = record[0]
                authoritative_inspection_date = record[1]
                authoritative_inspection_score = record[2]
                authoritative_inspection_type = record[3]
                authoritative_violation_id = record[4]
                authoritative_violation_description = record[5]
                authoritative_risk_category = record[6]
                values_i = (authoritative_business_name,
                            authoritative_business_address,
                            authoritative_inspection_id,
                            authoritative_inspection_date,
                            authoritative_inspection_score,
                            authoritative_inspection_type,
                            authoritative_violation_id,
                            authoritative_violation_description,
                            authoritative_risk_category)
                cur.execute(query_11, (values_i,))
                self.conn.commit()


    def clean_dirty_inspection(self):
        '''
        Performs all cleaning and uploading. NOTE: this function also performs
        the role of implementing a blocking solution to determine a set of
        promising candidate matches (find_cands), since cleaning and uploading
        occurs one "block" at a time.

        Order of operations:
        1. Clean records
        2. Determine potential matches (blocking solution)
        3. Compute similarity scores
        4. Determine matches
        5. Update authoritative restaurant and inspection tables accordingly
        '''
        query_1 = """DROP VIEW IF EXISTS zip_table;"""
        query_2 = """CREATE VIEW zip_table AS
                     SELECT
                     unique_id
                     , regexp_replace(lower(business_name),
                     '[^a-zA-Z0-9\s]+|\y(and\s|or\s|if\s|the\s)\y', '','g')
                        AS business_name
                     , regexp_replace(lower(business_address),
                     '[^a-zA-Z0-9\s]+','','g')
                        AS business_address
                     , trim('+' FROM business_phone_number)
                        AS business_phone_number
                     , business_postal_code
                     FROM sfinspection
                     WHERE business_postal_code = %s;"""
        query_3 = """SELECT *
                     FROM zip_table;"""
        query_4 = """DROP TABLE IF EXISTS cross_product;"""
        query_5 = """CREATE TEMPORARY TABLE cross_product (
                     unique_id_A                 int             ,
                     business_name_A             varchar(100)    ,
                     business_address_A          varchar(100)    ,
                     business_phone_number_A     varchar(20)     ,
                     unique_id_B                 int             ,
                     business_name_B             varchar(100)    ,
                     business_address_B          varchar(100)    ,
                     business_phone_number_B     varchar(20)     );"""
        query_6 = """INSERT INTO cross_product (unique_id_A, business_name_A,
                     business_address_A, business_phone_number_A, unique_id_B,
                     business_name_B, business_address_B,
                     business_phone_number_B)
                     SELECT
                         a.unique_id
                         , a.business_name AS business_name_A
                         , a.business_address AS business_address_A
                         , a.business_phone_number AS business_phone_number_A
                         , b.unique_id
                         , b.business_name AS business_name_B
                         , b.business_address AS business_address_B
                         , b.business_phone_number AS business_phone_number_B
                     FROM zip_table a
                     JOIN zip_table b
                     ON a.business_postal_code = b.business_postal_code
                     WHERE to_tsvector('english', LEFT(a.business_name, 2))
                                       @@ to_tsquery(%s)
                     AND to_tsvector('english', LEFT(b.business_name, 2))
                                     @@ to_tsquery(%s);"""
        query_7 = """SELECT * FROM cross_product;"""

        zip_codes = self.determine_zips()
        cur = self.conn.cursor()
        for code in zip_codes:
            cur.execute(query_1)
            cur.execute(query_2, code)
            cur.execute(query_3)
            two_letters = set()
            for record in cur:
                two_letters.add(record[1][:2])

            for pair in two_letters:
                cur.execute(query_4)
                cur.execute(query_5)
                cur.execute(query_6, (pair, pair))
                cur.execute(query_7)
                match_ids = set()
                non_match_ids = set()
                match_ids, non_match_ids = self.determine_match_nonmatch(cur,
                                           match_ids, non_match_ids)
                self.update_matches(cur, match_ids, non_match_ids)

        cur.close()
        self.conn.commit()

        return True


    def join_tables(self):
        logger.debug("Joining Trips")
        '''
        Joins cleaned restaurants, inspections, and bike rides, and finds
        bike trips ending within one week after a violation at restaurants
        (based on clean data) whose location is within 800
        meters of the trip destination.
        '''
        query_1 = """with sub (inspection_date, business_lat, business_long,
                               violation_id) AS (
                     SELECT
                         i.inspection_date as inspection_date
                         , r.business_latitude as business_latitude
                         , r.business_longitude as business_longitude
                         , i.violation_id as violation_id
                     FROM
                         cleaninspection i
                     LEFT JOIN
                         cleanrest r
                     ON i.business_name = r.business_name
                     WHERE
                         i.violation_id IS NOT NULL
                         AND r.business_latitude IS NOT NULL
                         AND r.business_longitude IS NOT NULL)
                    SELECT
                        t.duration_seconds as duration
                        , t.bike_id as bike_id
                        , s.violation_id as violation_id
                        , s.inspection_date as violation_date 
                    FROM
                        trips t
                    INNER JOIN
                        sub s
                    ON
                        t.end_station_latitude between
                        s.business_lat-.00720772 and 
                        s.business_lat+.00720772 AND
                        t.end_station_longitude between
                        s.business_long-0.0090810
                        and s.business_long+0.0090810
                    WHERE
                        t.end_time BETWEEN s.inspection_date
                        AND s.inspection_date + interval '7 days';"""
        query_2 = """INSERT INTO joinedinspbike VALUES %s
                     ON CONFLICT DO NOTHING RETURNING *;"""

        self.load_trips(file_path='trip.csv')
        cur = self.conn.cursor()
        cur.execute(query_1)
        records = cur.fetchall()
        for record in records:
            cur.execute(query_2, (record,))
        self.conn.commit()
        cur.close()

        return True


    # place holder test function
    def check_tables(self, list_of_sql):
        logger.debug("Checking Tables")
        cur = self.conn.cursor()
        res = []
        for sql in list_of_sql:
            cur.execute(sql)
            result = cur.fetchall()
            res.append(result)
        return res
