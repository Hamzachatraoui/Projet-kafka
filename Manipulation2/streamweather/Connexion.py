from pymysql import *

class Connexion(object):
    def __init__(self):
        try:
            # Establish connection to MySQL database
            self.dbconection = connect(
                host='localhost',
                port=3306,
                user='root',
                db='weather_data',
                password='Hamza@123'
            )
            self.dbcursor = self.dbconection.cursor()
        except Exception as e:
            print(f"Database connection error: {e}")

    def insert_weather_data(self, city, temperature, humidity, pressure, description, timestamp):
        try:
            query = """
            INSERT INTO weather_info (city, temperature_C, humidity, pressure, weather_description, timestamp)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            self.dbcursor.execute(query, (city, temperature, humidity, pressure, description, timestamp))
            self.dbconection.commit()  # Commit changes to the database
        except Exception as err:
            print(f"Failed to insert data into MySQL: {err}")

    def close_db(self):
        self.dbcursor.close()
        self.dbconection.close()
