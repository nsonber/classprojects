import mysql.connector as conn
import logging
import logging.handlers
import pymongo
import pandas as pd


def init_logger():
    """
        Static method to initialize the logger.
    """
    # self.lg.basicConfig(filename=r'.\logs\Fitbit.log', level=self.lg.INFO,
    #                format='%(asctime)s - %(levelname)s - %(message)s')

    LOG_FILE = r'.\logs\APITask.log'
    # logger for console and file
    logging.getLogger().setLevel(logging.INFO)
    lg = logging.getLogger()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # console
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(logging.INFO)
    # file
    filehandler = logging.FileHandler(LOG_FILE)
    filehandler.setFormatter(formatter)
    filehandler.setLevel(logging.INFO)

    lg.addHandler(console)
    lg.addHandler(filehandler)

    lg.info("----------------------------")
    return lg


class ApiTask:
    """
    Class for assignment given on 20-Aug-2022 for API
    Objective is to insert and fetch records from mysql/mongodb using API
    """

    def __init__(self):
        """
        Init method of the FitbitData class, which will invoke the logger and set the class attributes.
        """
        self.cursor = None
        self.db = None
        self.lg = init_logger()

    def get_sqldb_conn(self, username, password, dbname):
        """
        Function creates a connection to mysql database.
        It also creates and sets the database if not set already for the class object.

        :param username: Username of the mysql db to connect
        :param password: Password of mysql db
        :param dbname: Database name
        :return: True --> if successfully connected to database
                 False --> if database connection is not acquired
        """

        try:
            self.lg.info("Getting SQL DB connection")
            self.db = conn.connect(host='localhost',
                                   user=username,
                                   passwd=password
                                   )

            self.cursor = self.db.cursor(buffered=True)

            self.lg.info("Creating DB if not exist")
            self.cursor.execute(f"create database if not exists {dbname}")

            self.cursor.execute(f"use {dbname}")

        except Exception as e:
            self.lg.error("Error encountered while acquiring SQL DB connection")
            self.lg.error(e)
            return False

        self.lg.info("SQL DB connection acquired")

        return True

    def close_db(self):
        self.db.close()

    def get_nosql_db(self, username, password, dbname, collection):
        """
        Method creates a collection in MongoDb and returns
        :return:
        """
        self.lg.info("Getting Mongo DB connection")
        hostname = 'cluster0.iertm.mongodb.net/?retryWrites=true&w=majority'
        conn_str = "mongodb+srv://" + username + ":" + password + "@" + hostname
        client = pymongo.MongoClient(conn_str)
        db = client[dbname]
        coll = db[collection]
        self.lg.info("Mongo DB connection acquired")
        return coll

    def insert_into_mysql(self, d_rec):
        """
        Function inserts a single record into mysql table.
        :return:
        """

        self.get_sqldb_conn(username='root', password="Mysql@Jul22", dbname='FitBit')

        try:
            self.cursor.execute(
                'INSERT INTO FITBIT_DATA (ID, ACTIVITYDATE, TOTALSTEPS, TOTALDISTANCE'
                ', TRACKERDISTANCE, LOGGEDACTIVITIESDISTANCE, VERYACTIVEDISTANCE, MODERATELYACTIVEDISTANCE'
                ', LIGHTACTIVEDISTANCE, SEDENTARYACTIVEDISTANCE, VERYACTIVEMINUTES, FAIRLYACTIVEMINUTES'
                ', LIGHTLYACTIVEMINUTES, SEDENTARYMINUTES, CALORIES)'
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                (d_rec['Id'], pd.to_datetime(d_rec['ActivityDate']), d_rec['TotalSteps'], d_rec['TotalDistance'],
                 d_rec['TrackerDistance'], d_rec['LoggedActivitiesDistance'], d_rec['VeryActiveDistance'],
                 d_rec['ModeratelyActiveDistance'], d_rec['LightActiveDistance'], d_rec['SedentaryActiveDistance'],
                 d_rec['VeryActiveMinutes'], d_rec['FairlyActiveMinutes'], d_rec['LightlyActiveMinutes'],
                 d_rec['SedentaryMinutes'], d_rec['Calories']
                 )
            )

            self.cursor.execute('commit')

            self.lg.info(f"Inserted records to FitBit_data")
            self.close_db()

        except Exception as e:
            self.lg.error("Error encountered while inserting data")
            self.lg.error(str(e))
            self.close_db()
            return False
        return True

    def insert_to_mongodb(self, record):
        """
        Function inserts single record to MongoDB
        :return:
        """
        coll = self.get_nosql_db('ineuronmongo', 'ineuron1', 'FitBit', 'FitBitData')
        try:
            coll.insert_one(record)

            self.lg.info(f"Inserted records to MongoDB")
        except Exception as e:
            self.lg.error("Error encountered while inserting data to MongoDB")
            self.lg.exception(e)
            return False

        return True

    def get_record_mysql(self, Id, activitydate):

        self.get_sqldb_conn(username='root', password="Mysql@Jul22", dbname='FitBit')

        statement = f"select ID, ACTIVITYDATE, TOTALSTEPS from fitbit_data " \
                    f"where id = {Id} AND ActivityDate = '{activitydate}'"

        self.lg.info(f"Statement getting executed {statement}")

        self.cursor.execute(statement)
        records = self.cursor.fetchall()

        self.lg.info(records)

        self.close_db()
        return records

    def get_record_from_mongo(self, Id):
        """
        Function inserts single record to MongoDB
        :return:
        """
        coll = self.get_nosql_db('ineuronmongo', 'ineuron1', 'FitBit', 'FitBitData')
        intId = int(Id)
        recs = []
        try:
            records = coll.find({
                "Id": {"$eq": intId}
            })

            for rec in records:
                recs.append(rec)

            self.lg.info(f"Fetched records from MongoDB")
        except Exception as e:
            self.lg.error("Error encountered while fetching data to MongoDB")
            self.lg.exception(e)
            return "Exception"

        return recs

    def update_into_mysql(self, d_data):

        self.get_sqldb_conn(username='root', password="Mysql@Jul22", dbname='FitBit')

        part_stmt = ''
        i = 0
        for key, value in d_data.items():
            i += 1
            if key == 'Id':
                intID = int(value)
            elif key == 'ActivityDate':
                Activity_Date = pd.to_datetime(value)
            else:
                part_stmt = part_stmt + f" {key} = {value}"

            if i != len(d_data) and len(part_stmt) != 0:
                part_stmt = part_stmt + ','

        statement = f"UPDATE fitbit_data SET  {part_stmt} WHERE " \
                    f" Id = {intID} AND ActivityDate = '{Activity_Date}'"

        try:
            self.cursor.execute("SET SQL_SAFE_UPDATES = 0")

            self.cursor.execute(statement)

            self.cursor.execute("commit")
            self.close_db()

        except Exception as e:
            self.lg.error(f"Error encountered while updating data using statement {statement}")
            self.lg.error(str(e))
            self.close_db()
            return False
        return True

    def delete_data_mysql(self, d_data):

        self.get_sqldb_conn(username='root', password="Mysql@Jul22", dbname='FitBit')

        intID = 0
        Activity_Date = ''
        for key, value in d_data.items():
            if key == 'Id':
                intID = int(value)
            elif key == 'ActivityDate':
                Activity_Date = pd.to_datetime(value)

        if intID == 0 and len(Activity_Date) == 0:
            self.lg.error("Key Column not as an input for Delete Operation")
            return False

        statement = f"DELETE FROM  fitbit_data WHERE " \
                    f" Id = {intID} AND ActivityDate = '{Activity_Date}'"

        try:
            # self.cursor.execute("SET SQL_SAFE_UPDATES = 0")

            self.cursor.execute(statement)

            self.cursor.execute("commit")
            self.close_db()

        except Exception as e:
            self.lg.error(f"Error encountered while updating data using statement {statement}")
            self.lg.error(str(e))
            self.close_db()
            return False
        return True

    def update_into_mongo(self, d_data):
        """
        Function inserts single record to MongoDB
        :return:
        """
        coll = self.get_nosql_db('ineuronmongo', 'ineuron1', 'FitBit', 'FitBitData')

        part_stmt = ''
        i = 0
        for key, value in d_data.items():
            i += 1
            if key == 'Id':
                intId = int(value)
            elif key == 'ActivityDate':
                Activity_Date = pd.to_datetime(value)
            else:
                part_stmt = part_stmt + f" {key} : {value} "

            if i != len(d_data) and len(part_stmt) != 0:
                part_stmt = part_stmt + ','

        self.lg.info(part_stmt)

        try:
            coll.update_many(
                {"Id": {"$eq": intId}},
                {"$set": {part_stmt}}
            )

            self.lg.info(f"Updated records to MongoDB")
        except Exception as e:
            self.lg.error("Error encountered while updating data to MongoDB")
            self.lg.exception(e)
            return False

        return True

    def delete_data_mongo(self, d_data):
        """
        Function Deletes records in MongoDB
        :return:
        """
        coll = self.get_nosql_db('ineuronmongo', 'ineuron1', 'FitBit', 'FitBitData')

        intId = int(d_data["Id"])

        try:
            coll.delete_many({"Id": {"$eq": intId}})

            self.lg.info(f"Deleted records to MongoDB")
        except Exception as e:
            self.lg.error("Error encountered while deleting data in MongoDB")
            self.lg.exception(e)
            return False

        return True
