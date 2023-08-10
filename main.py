"""
SQL ALCHEMY-ORM code
** Make sure you make the following changes **
1. Change the username and password of orm_connection() to your own username and
   password.
2. Change the mysql url link corresponds to your database.
   For example, "mysql://%s:%s@localhost/<yourDBname>". Make sure your DB exists.
3. Make sure you give your CSV_FILE_PATH as input.
"""
import time

from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import csv
import json
import os

CSV_FILE_PATH = '/tmp/weather_events.csv'
JSON_FILE_PATH = '/tmp/weather_events.json'

Base = declarative_base()


# Define ORM mapping
class Airport(Base):
    __tablename__ = 'airport'

    airport_code = Column(String(10), primary_key=True)
    state = Column(String(45))
    county = Column(String(45))
    city = Column(String(45))
    zipcode = Column(Integer)
    locationLat = Column(Float)
    locationLng = Column(Float)


class Weather(Base):
    __tablename__ = 'weather'

    airport_code = Column(String(50), primary_key=True)
    start_time = Column(DateTime, primary_key=True)
    end_time = Column(DateTime, primary_key=True)
    weather_type = Column(String(50))
    weather_severity = Column(String(50))
    precipitation = Column(Float)


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
        # Convert each row into a dictionary
        # and add it to data
        idx = 0
        # idx identifies the key of json data.
        for rows in csv_reader:
            data[idx] = rows
            idx += 1

    # data is a dictionary, we need to convert it to a json string.
    # How to convert it? - using json.dumps(), output is json string.
    with open(JSON_FILE_PATH, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))


def orm_connection():
    # Make sure SQL Alchemy connection is successful
    # username = input("Enter username: ")
    # password = getpass("Enter password: ")
    username, password = 'root', 'MariaProject'
    # Set echo flag if you want to see the logs.
    engine = create_engine("mysql://%s:%s@localhost/justinedb" % (username, password),
                           echo=False)
    return engine


def get_json_data_as_dict():
    # Checking whether json file exists or not.
    if not os.path.isfile(JSON_FILE_PATH):
        print("JSON file not found for conversion")
        return
    # Reading the JSON file contents.
    with open(JSON_FILE_PATH, 'r') as fd:
        json_str = fd.read()

    # Make sure json contents are not empty.
    assert json_str, "JSON content cannot be empty"

    # Ideally, this should be in the try/except block.
    # Why try/except block?
    # what if json_op is not a json formatted String(50)?
    try:
        # json.loads() converts json string to python dict.
        weather_dict = json.loads(json_str)
    except ValueError as e:
        print("Error observed: %s" % str(e))
        weather_dict = {}
    return weather_dict


def create_tables(engine):
    Base.metadata.drop_all(bind=engine)
    print("Drop table successful")
    Base.metadata.create_all(engine)
    print("Table creation successful")


def populate_tables(engine, weather_dict):
    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    def populate_airport_table():
        data = list()
        # Data-structure to eliminate duplicate airport code entries.
        airport_code_cache = set()
        for k, weather_info in weather_dict.items():
            airport_code = weather_info['AirportCode']
            if airport_code in airport_code_cache:
                continue
            airport_code_cache.add(airport_code)

            data.append({"airport_code": weather_info['AirportCode'],
                         "state": weather_info['State'],
                         "county": weather_info['County'],
                         "city": weather_info['City'],
                         "zipcode": weather_info['ZipCode'] if weather_info['ZipCode'] else None,
                         "locationLat": weather_info['LocationLat'],
                         "locationLng": weather_info['LocationLng']})

        session.bulk_insert_mappings(Airport, data, render_nulls=True)
        session.commit()
        print("airport table insertion successful")

    def populate_weather_table():
        data = list()
        for k, weather_info in weather_dict.items():
            data.append({"airport_code": weather_info['AirportCode'],
                         "start_time": weather_info['StartTime(UTC)'],
                         "end_time": weather_info['EndTime(UTC)'],
                         "weather_type": weather_info['Type'],
                         "weather_severity": weather_info['Severity'],
                         "precipitation": weather_info['Precipitation(in)']
                         })
        session.bulk_insert_mappings(Weather, data)
        session.commit()
        print("weather table insertion successful")
    populate_airport_table()
    populate_weather_table()


def main():
    engine = orm_connection()
    assert engine, "engine object cannot be None"
    convert_csv_file_to_json_file()
    weather_dict = get_json_data_as_dict()
    assert weather_dict, 'weather info is not retrieved'

    create_tables(engine)
    start_time = time.time()
    populate_tables(engine, weather_dict)
    print("Took %ss" % str(time.time()-start_time))


if __name__ == "__main__":
    main()
