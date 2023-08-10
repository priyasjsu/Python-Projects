"""
JDBC-Python code
** Make sure you make the following changes **
1. Change the username and password of orm_connection() to your own username and
   password.
2. Change the mysql url link corresponds to your database.
   For example, "mysql://%s:%s@localhost/<yourDBname>". Make sure your DB exists.
3. Make sure you give your CSV_FILE_PATH as input.
"""
import csv
import time

import jaydebeapi
import json
import os

JSON_FILE_PATH = '/tmp/weather_events.json'
CSV_FILE_PATH = '/tmp/weather_events.csv'
BATCH_SIZE = 500000


def convert_csv_file_to_json_file():
    data = {}
    if os.path.isfile(JSON_FILE_PATH):
        print("JSON file already exists")
        return
    if not os.path.isfile(CSV_FILE_PATH):
        print("CSV file doesn't exist")
        return
    with open(CSV_FILE_PATH, encoding='utf-8') as csvf:
        csv_reader = csv.DictReader(csvf)
        idx = 0
        for rows in csv_reader:
            data[idx] = rows
            idx += 1

    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))


def get_json_data_as_dict():
    if not os.path.isfile(JSON_FILE_PATH):
        print("JSON file not found for conversion")
        return
    with open(JSON_FILE_PATH, 'r') as fd:
        json_str = fd.read()

    assert json_str, "JSON content cannot be empty"

    try:
        weather_dict = json.loads(json_str)
    except ValueError as e:
        print("Error observed: %s" % str(e))
        weather_dict = {}
    return weather_dict


def create_jdbc_connection():
    conn = jaydebeapi.connect('com.mysql.cj.jdbc.Driver',
                              'jdbc:mysql://localhost:3306/justinedb',
                              ["root", "MariaProject"],
                              '/Users/justinej/Downloads/mysql-connector-'
                              'java-8.0.29/mysql-connector-java-8.0.29.jar')
    return conn


def drop_tables(conn):
    curs = conn.cursor()
    try:
        curs.execute("show tables")
        table_op = curs.fetchall()
        if not table_op:
            return
        for table in table_op:
            curs.execute("DROP TABLE %s" % table[0])
        conn.commit()
    except Exception as e:
        print("Drop table operation failed: %s" % str(e))
        raise
    finally:
        curs.close()
        print("Drop tables successful")


def create_tables(conn):
    # Create airport table
    with open('airport_tbl_create.txt', 'r') as fd:
        airport_tbl_create_stmt = fd.read()

    # Create weather table
    with open("weather_tbl_create.txt", 'r') as fd:
        weather_tbl_create_stmt = fd.read()

    try:
        curs = conn.cursor()
        curs.execute(airport_tbl_create_stmt)
        curs.execute(weather_tbl_create_stmt)
        conn.commit()
    except Exception as e:
        print("Exception occurred on airport table creation: %s" % str(e))
        raise
    finally:
        curs.close()
        print("Create tables successful")


def populate_tables(conn, weather_dict):
    def populate_airport_table():
        airport_tbl_insert_stmt = "INSERT INTO airport (airport_code, state, " \
                                  "county, city, zipcode, `locationLat`, " \
                                  "`locationLng`) VALUES (?, ?, ?, ?, ?, ?, ?)"
        data_list = []
        # Data structure to eliminate duplicate airport code entries.
        airport_code_cache = set()

        for k, weather_info in weather_dict.items():
            airport_code = weather_info['AirportCode']
            if airport_code in airport_code_cache:
                continue
            airport_code_cache.add(airport_code)
            data_list.append((weather_info['AirportCode'],
                             weather_info['State'],
                             weather_info['County'],
                             weather_info['City'],
                             weather_info['ZipCode'] if weather_info['ZipCode']
                              else None,
                             weather_info['LocationLat'],
                             weather_info['LocationLng']))
        if not data_list:
            print("No data to insert for airport table")
            return

        curs.executemany(airport_tbl_insert_stmt, data_list)
        conn.commit()
        print("airport table insertion successful")

    def populate_weather_table():
        weather_tbl_insert_stmt = "INSERT INTO weather (airport_code, start_time, " \
                                  "end_time, weather_type, weather_severity, " \
                                  "precipitation) VALUES (?, ?, ?, ?, ?, ?)"
        data_list = []
        count = 0
        for k, weather_info in weather_dict.items():
            data_list.append((weather_info['AirportCode'],
                              weather_info['StartTime(UTC)'],
                              weather_info['EndTime(UTC)'],
                              weather_info['Type'],
                              weather_info['Severity'],
                              weather_info['Precipitation(in)']))
            count += 1
            if count == BATCH_SIZE:
                curs.executemany(weather_tbl_insert_stmt, data_list)
                conn.commit()
                data_list.clear()
                count = 0
        if data_list:
            curs.executemany(weather_tbl_insert_stmt, data_list)
            conn.commit()
        print("weather table insertion successful")

    try:
        curs = conn.cursor()
        populate_airport_table()
        populate_weather_table()
    except Exception as e:
        print("Error encountered on table insertion: %s" % str(e))
        raise
    finally:
        curs.close()


def main():
    conn = create_jdbc_connection()
    assert conn, "JDBC connection object shouldn't be none"
    conn.jconn.setAutoCommit(False)
    drop_tables(conn)
    create_tables(conn)

    convert_csv_file_to_json_file()
    weather_dict = get_json_data_as_dict()
    assert weather_dict, 'weather info cannot be None'

    start_time = time.time()
    populate_tables(conn, weather_dict)
    print("Took %ss" % str(time.time()-start_time))

    conn.close()


if __name__ == "__main__":
    main()
