import json, logging, os,sys
import mysql.connector

def db_connector(func):
    """
    Create ODBC decorator for Database connections using mySQL client.
    Abstracts ODBC logic away from core business code.
    Also when this app needs to scale it easy to build extra connectors for additional and distinct DB source
    https://medium.com/opex-analytics/database-connections-in-python-extensible-reusable-and-secure-56ebcf9c67fe

    :param func:
    :return: conn -> mySQL ODBC client
    """
    def load_config_file_(path="./database_config.json"):
        """
        Load database config from json file first, everytime a connection is made.
        :param path: Default path point to root, but can be overriden
        :return: config -> Dict()
        """
        try:
            with open(path, "r+") as file:
                return json.load(file)
        except FileExistsError:
            logging.info(f"File does not exist at {path}")
            return None
        except FileNotFoundError:
            logging.info(f"File not found at directory {path}")
            return None
        except Exception as e:
            logging.error(f"File error", exc_info=e)
            sys.exit() # Prefer to exit the program, as credentials might be provided, but file might contain abnormalities that will need correction first

    def with_connection_(*args,**kwargs):
        """

        :param args:
        :param kwargs:
        :return rv:
        """
        config = load_config_file_()
        if config is None:
            """
            If no json file for configuration is provided look in the environment settings 
            for database credentials
            """
            config = {
                "username": os.getenv("username"),
                "password": os.getenv("password"),
                "server": os.getenv("server"),
                "port": os.getenv("port",3306),
                "database": os.getenv("database"),
            }
        elif config == {}:
            logging.error("No Database connector credentials provided")
            sys.exit()

        try:
            cnn = mysql.connector.connect(user=config.get("username", "sa"), \
                                          host=config.get("server", "localhost"), \
                                          port=config.get("port", 3306), \
                                          password=config.get("password", "p@ssw0rd100"), \
                                          database=config.get("database"))
            rv = func(cnn, *args,**kwargs)
        except mysql.connector.Error as e:
            logging.error("Database connection error", exc_info=e)
            try:
                cnn.rollback() # Rollback any DB transactions if connection was successful
            except:
                return None # Fail before asking permission to fail on connection existence
        except Exception as e:
            logging.error("Unforeseen Error", exc_info=e)
            raise
        else:
            cnn.commit()
        finally:
            try:
                cnn.close()
            except:
                return None
        return rv
    return with_connection_