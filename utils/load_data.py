from utils.database import connect_to_mysql, connect_to_mongodb, create_database_mysql,\
    create_database_mongodb
from concurrent.futures import ThreadPoolExecutor
from threading import Lock, Event

import uuid
import json
import os

from datetime import datetime as dt
from typing import Dict, Tuple
from time import sleep, time

# Connect to the MongoDB and MySQL
MONGO_CLIENT = connect_to_mongodb()
MYSQL_CONN = connect_to_mysql()

# Filename to Item categories
file2category = {'Amazon_Instant_Video_5.json': 'Instant video',
                 'Digital_Music_5.json': 'Digital music',
                 'Grocery_and_Gourmet_Food_5.json': 'Grocery',
                 'Musical_Instruments_5.json': 'Musical Instruments',
                 'Office_Products_5.json': 'Office',
                 'Sports_and_Outdoors_5.json': 'Sports and Outdoors',
                 'Toys_and_Games_5.json': 'Toys and Games',
                 'Video_Games_5.json': 'Video games'}


# Progress bar for data loading
class ProgressBar:
    """
    A class to create a progress bar for any iterable in Python.
    """

    def __init__(self, total, length=40, fill_char='█', empty_char='-', prefix='Progress:', suffix='Complete',
                 decimals=1):
        """
        Initialize the progress bar with the total number of items in the iterable and optional parameters.

        Parameters:
            total (int): The total number of items in the iterable.
            length (int): The length of the progress bar in characters (default: 40).
            fill_char (str): The character used to fill the progress bar (default: '█').
            empty_char (str): The character used to represent empty space in the progress bar (default: '-').
            prefix (str): The prefix to display before the progress bar (default: 'Progress:').
            suffix (str): The suffix to display after the progress bar (default: 'Complete').
            decimals (int): The number of decimal places to display in the completion percentage (default: 1).
        """
        self.total = total
        self.length = length
        self.fill_char = fill_char
        self.empty_char = empty_char
        self.prefix = prefix
        self.suffix = suffix
        self.decimals = decimals
        self.iteration = 0
        self.percent = 0

    def __next__(self):
        """
        Update the progress bar with the next iteration count and return the next item in the iterable.
        """
        self.iteration += 1
        return self.update(self.iteration)

    def __enter__(self):
        """
        Set up the progress bar for use with a 'with' statement.

        Returns:
            self: The ProgressBar instance.
        """
        return self

    def update(self, iteration):
        """
        Update the progress bar with the current iteration count.

        Parameters:
            iteration (int): The current iteration count.
        """
        self.percent = 100 * (iteration / float(self.total))
        filled_length = int(self.length * iteration // self.total)
        bar = self.fill_char * filled_length + self.empty_char * (self.length - filled_length)
        message = f'{self.prefix} |{bar}| {self.percent:.{self.decimals}f}% {self.suffix}'
        print('\r' + message, end='')

        if iteration == self.total:
            print('')

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Clean up the progress bar after use with a 'with' statement.
        """
        pass


# Spinning wheel animation
def animate(stop_event, text):
    while not stop_event.is_set():
        for c in '|/-\\':
            print(f"\r{text} {c}", end="")
            sleep(0.1)


# Loads all data from specified directory
def _load_items(path_to_files="data"):
    # Define a list to store the JSON data from all files
    data = []

    # Start spinning wheel animation in a separate thread
    with ThreadPoolExecutor() as executor:
        stop_event = Event()
        future = executor.submit(animate, stop_event, 'Loading reviews')

        # Loop through each file
        for filename in os.listdir(path_to_files):
            category = file2category.get(filename, None)
            # Load the JSON data from the file (newline-delimited JSON)
            with open(os.path.join(path_to_files, filename), 'r') as f:
                for line in f:
                    obj = json.loads(line)
                    obj['category'] = category
                    # Append the JSON data to the list
                    data.append(obj)

        # Stop spinning wheel animation
        stop_event.set()

    data.append({
        "reviewerID": "A3_B1S8AL_6V2A4", "asin": "5555991584",
        "reviewerName": "David Bisbal", "helpful": [12, 12],
        "reviewText": "¿Cómo están los máquinas? Lo primero de todo, ¿nos hacemos unas fotillos o qué?",
        "overall": 5.0,"summary": "¿Como estan los máquinas?",
        "unixReviewTime": 1084226400,
        "reviewTime": '05 11, 2004',
        'category': 'Digital music'
        }
)
    print("\rCompleted loading reviews")
    return data


# Get requested information about users, items and reviews
def _get_users_items_reviews(reviews, user_details=('reviewerID', 'reviewerName'),
                             item_details=('asin', 'category'),
                             review_details=('reviewText', 'helpful', 'overall', 'summary', 'unixReviewTime',
                                             'reviewTime', 'category'),
                             pbar=None):
    users_list, items_list, reviews_list = [], [], []
    for review in reviews:
        # Create unique uuids
        user_uuid = uuid.uuid4()
        item_uuid = uuid.uuid4()
        review_uuid = uuid.uuid4()

        # Extract review information
        review_info = {
            'id': str(review_uuid),
            'reviewer_id': None,
            'item_id': None
        }
        for detail in review_details:
            if detail in ['id', 'reviewer_id', 'item_id']:
                continue
            elif detail in ['reviewTime']:
                strdate = review.get(detail, None)
                if strdate is not None:
                    review_info[detail] = dt.strptime(strdate, '%m %d, %Y')
                else:
                    review_details[detail] = None
            else:
                review_info[detail] = review.get(detail, None)

        # Extract user information
        user_id = review.get('reviewerID', None)
        user_ids_lock.acquire()
        already_registered_user = user_id in user_ids
        user_ids_lock.release()

        if not already_registered_user:
            user_info = {
                'id': str(user_uuid)
            }
            # Add requested user details
            for detail in user_details:
                if detail == 'id':
                    continue
                else:
                    user_info[detail] = review.get(detail, None)

            user_ids_lock.acquire()
            user_ids[user_id] = str(user_uuid)
            user_ids_lock.release()
            users_list.append(user_info)

        review_info['reviewer_id'] = user_ids.get(user_id) if already_registered_user else str(user_uuid)

        # Extract item information
        item_id = review.get('asin', None)
        item_ids_lock.acquire()
        already_registered_item = item_id in item_ids
        item_ids_lock.release()

        if not already_registered_item:
            item_info = {
                'id': str(item_uuid)
            }
            # Add requested item details
            for detail in item_details:
                if detail == 'id':
                    continue
                else:
                    item_info[detail] = review.get(detail, None)

            item_ids_lock.acquire()
            item_ids[item_id] = str(item_uuid)
            item_ids_lock.release()
            items_list.append(item_info)

        review_info['item_id'] = item_ids.get(item_id) if already_registered_item else str(item_uuid)
        reviews_list.append(review_info)
        if pbar:
            next(pbar)
    return users_list, items_list, reviews_list


# Save users, items and reviews to databases
def _save_data(users, items, reviews, mysql_db_name='amz_reviews', mongo_db_name='amz_reviews',
               user_details=None, item_details=None):

    if item_details is None:
        item_details = {'asin': 'VARCHAR(255)', 'category': 'VARCHAR(255)'}
    if user_details is None:
        user_details = {'reviewerID': 'VARCHAR(255)', 'reviewerName': 'VARCHAR(255)'}

    # Save users and items to SQL database
    mysql_db_name = create_database_mysql(mysql_db_name, user_details, item_details)
    cursor = MYSQL_CONN.cursor()
    cursor.execute(f"USE {mysql_db_name}")

    with ThreadPoolExecutor() as executor:
        stop_event = Event()
        future = executor.submit(animate, stop_event, f'Saving users and items in {mysql_db_name} (MySQL)')
        # Insert users data into users table
        user_columns = ', '.join(['id'] + list(user_details))
        user_values_template = ', '.join(['%s'] * (len(user_details) + 1))
        user_values = [tuple(user.values()) for user in users]
        cursor.executemany(f"INSERT INTO users ({user_columns}) VALUES ({user_values_template})", user_values)

        # Insert items data into items table
        item_columns = ', '.join(['id'] + list(item_details))
        item_values_template = ', '.join(['%s'] * (len(item_details) + 1))
        item_values = [tuple(item.values()) for item in items]
        cursor.executemany(f"INSERT INTO items ({item_columns}) VALUES ({item_values_template})", item_values)

        # Commit new insertions
        MYSQL_CONN.commit()

        # Stop spinning wheel animation
        stop_event.set()
    print(f"\rCompleted saving users and items in {mysql_db_name} (MySQL)")

    # Save review details to "reviews" collection.
    mongo_db_name = create_database_mongodb(mongo_db_name)
    mongo_database = MONGO_CLIENT[mongo_db_name]
    reviews_col = mongo_database['reviews']

    with ThreadPoolExecutor() as executor:
        stop_event = Event()
        future = executor.submit(animate, stop_event, f'Saving reviews in {mongo_db_name} (MongoDB)')
        # Insert reviews data into reviews collection
        reviews_col.insert_many(reviews)
        # Stop spinning wheel animation
        stop_event.set()
    print(f"\rCompleted saving reviews in {mongo_db_name} (MongoDB)")
    return mysql_db_name, mongo_db_name


# Worker Thread function
def _worker(reviews, user_details=('reviewerID', 'reviewerName'),
            item_details=('asin', 'category'),
            review_details=('reviewText', 'helpful', 'overall', 'summary', 'unixReviewTime',
                            'reviewTime', 'category'),
            pbar=None):
    return _get_users_items_reviews(reviews, user_details=user_details, item_details=item_details,
                                    review_details=review_details,
                                    pbar=pbar)


# Global variables shared by threads
user_ids = dict()
item_ids = dict()
user_ids_lock = Lock()
item_ids_lock = Lock()


# Main function
def etl(path_to_files: str = 'data', user_details: Dict[str, str] = None, item_details: Dict[str, str] = None,
        review_details: Tuple[str] = ('reviewText', 'helpful', 'overall', 'summary', 'unixReviewTime',
                                      'reviewTime', 'category'),
        mysql_db_name: str = 'amz_reviews', mongo_db_name: str = 'amz_reviews',
        workers: int = 4):

    if item_details is None:
        item_details = {'asin': 'VARCHAR(255)', 'category': 'VARCHAR(255)'}
    if user_details is None:
        user_details = {'reviewerID': 'VARCHAR(255)', 'reviewerName': 'VARCHAR(255)'}
    try:
        reviews = _load_items(path_to_files=path_to_files)
        num_reviews_per_chunk = len(reviews) // workers

        chunks = []
        t = None
        for t in range(workers - 1):
            chunks.append(reviews[t * num_reviews_per_chunk:(t + 1) * num_reviews_per_chunk])
        last_chunk = reviews[(t + 1) * num_reviews_per_chunk:]
        chunks.append(last_chunk)

        with ThreadPoolExecutor(max_workers=workers) as executor, \
                ProgressBar(len(reviews), prefix="Processing reviews:") as pbar:
            results = [executor.submit(_worker, chunk, user_details.keys(), item_details.keys(), review_details,
                                       pbar) for chunk in chunks]
    finally:
        # Merge the results from all worker threads
        users_list, items_list, reviews_list = [], [], []
        for result in results:
            users, items, reviews = result.result()
            users_list.extend(users)
            items_list.extend(items)
            reviews_list.extend(reviews)
        # Save the results to disk
        mysql_db_name, mongo_db_name = _save_data(users=users_list, items=items_list, reviews=reviews_list,
                                                  mysql_db_name=mysql_db_name, mongo_db_name=mongo_db_name,
                                                  user_details=user_details, item_details=item_details)
    return mysql_db_name, mongo_db_name
