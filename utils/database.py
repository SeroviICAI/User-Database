import pymongo
import mysql.connector
import neo4j

import configparser
from typing import Dict

# Extract MongoClient parameters from `config.ini` file
config = configparser.ConfigParser()
config.read('config.ini')
if config['MongoDB']['user'] and config['MongoDB']['password']:
    uri = f"mongodb://{config['MongoDB']['user']}:" \
          f"{config['MongoDB']['password']}" \
          f"@{config['MongoDB']['server']}:{config['MongoDB']['port']}/"
else:
    uri = f"mongodb://{config['MongoDB']['server']}:{config['MongoDB']['port']}/"

# Connect to the MongoDB server
MONGO_URI = uri

# Connect to SQL
MYSQL_HOST = config['MySQL']['host']
MYSQL_USER = config['MySQL']['user']
MYSQL_PASSWORD = config['MySQL']['password']

# Connect to Neo4j
NEO4J_URI = f"bolt://{config['Neo4j']['server']}:{config['Neo4j']['port']}"
NEO4J_USER = config['Neo4j']['user']
NEO4J_PASSWORD = config['Neo4j']['password']

__all__ = [
    'connect_to_mongodb',
    'connect_to_mysql',
    'create_database_mysql',
    'create_database_mongodb'
]
mongo_client: pymongo.MongoClient = None
mysql_conn: mysql.connector.MySQLConnection = None
neo4j_driver: neo4j.Driver = None


def connect_to_mongodb() -> pymongo.MongoClient:
    global mongo_client
    if not mongo_client:
        mongo_client = pymongo.MongoClient(MONGO_URI)
    return mongo_client


def connect_to_mysql() -> mysql.connector.MySQLConnection:
    global mysql_conn
    if not mysql_conn:
        mysql_conn = mysql.connector.connect(host=MYSQL_HOST,
                                             user=MYSQL_USER,
                                             password=MYSQL_PASSWORD)
    return mysql_conn


def connect_to_neo4j() -> neo4j.Driver:
    global neo4j_driver
    if not neo4j_driver:
        neo4j_driver = neo4j.GraphDatabase.driver(NEO4J_URI,
                                                  auth=(NEO4J_USER,
                                                        NEO4J_PASSWORD))
    return neo4j_driver


class NoClientConnected(Exception):
    def __init__(self, message="No client was connected. Please connect to your client first before creating a "
                               "database."):
        self.message = message
        super().__init__(self.message)


def create_database_mysql(name: str, user_details: Dict[int, int], item_details: Dict[int, int]) -> str:
    # Check if there is a connection to MySQL server
    if mysql_conn is None:
        raise NoClientConnected("No MySQL server was connected. Please connect to your client first before creating a "
                                "database.")
    cursor = mysql_conn.cursor()

    # Check if database with same name already exists
    cursor.execute("SHOW DATABASES")
    db_exists = False
    for db in cursor:
        if db[0] == name:
            db_exists = True
            break

    # If database with same name exists, prompt user for action
    if db_exists:
        print(f"Warning: A MySQL database with the name {name} already exists.")
        action = input(
            "Enter 'd' to drop the existing database or 'c' to create a new database with a different name: ")
        # Consume any unread results
        cursor.fetchall()
        if action == 'd':
            # Drop the existing database
            cursor.execute(f"DROP DATABASE {name}")
            print(f"Database {name} dropped.")
        elif action == 'c':
            n = 1
            new_db_name = f"{name}_{n}"
            while True:
                cursor.execute("SHOW DATABASES")
                name_exists = False
                for db in cursor:
                    if db[0] == new_db_name:
                        name_exists = True
                        break
                if not name_exists:
                    break
                n += 1
                new_db_name = f"{name}_{n}"
                # Consume any unread results
                cursor.fetchall()
            name = new_db_name
            print(f"Creating new database with name {name}.")

    # Create database if it doesn't exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {name}")

    # Use the created database
    cursor.execute(f"USE {name}")

    # Create users table
    user_columns = ', '.join([f"{col} {dtype}" for col, dtype in user_details.items()])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS users (id VARCHAR(255) PRIMARY KEY, {user_columns})")

    # Create items table
    item_columns = ', '.join([f"{col} {dtype}" for col, dtype in item_details.items()])
    cursor.execute(f"CREATE TABLE IF NOT EXISTS items (id VARCHAR(255) PRIMARY KEY, {item_columns})")
    return name


def create_database_mongodb(name) -> str:
    if mongo_client is None:
        raise NoClientConnected("No MongoDB server was connected. Please connect to your client first before creating "
                                "a database.")
    # Check if database with same name already exists
    db_exists = False
    for db in mongo_client.list_database_names():
        if db == name:
            db_exists = True
            break

    # If database with same name exists, prompt user for action
    if db_exists:
        print(f"Warning: A MongoDB database with the name {name} already exists.")
        action = input(
            "Enter 'd' to drop the existing database or 'c' to create a new database with a different name: ")
        if action == 'd':
            mongo_client.drop_database(name)
            print(f"Database {name} dropped.")
        elif action == 'c':
            n = 1
            new_db_name = f"{name}_{n}"
            while True:
                name_exists = False
                for db in mongo_client.list_database_names():
                    if db == new_db_name:
                        name_exists = True
                        break
                if not name_exists:
                    break
                n += 1
                new_db_name = f"{name}_{n}"
            name = new_db_name
            print(f"Creating new database with name {name}.")
    return name
