from app.dash_app import launch_app
from utils.load_data import etl


if __name__ == '__main__':
    mysql_db_name, mongo_db_name = etl()
    launch_app(mysql_db_name, mongo_db_name)
