from app.dash_app import launch_app
from utils.load_data import etl


if __name__ == '__main__':
    etl()
    launch_app()
