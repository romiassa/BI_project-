import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

class DatabaseConnector:
    def __init__(self, config):
        self.config = config
    
    def connect_sql_server(self):
        """Connexion à SQL Server"""
        try:
            conn_str = (
                f"DRIVER={self.config['sql_server']['driver']};"
                f"SERVER={self.config['sql_server']['server']};"
                f"DATABASE={self.config['sql_server']['database']};"
                f"Trusted_Connection={self.config['sql_server']['trusted_connection']};"
            )
            return pyodbc.connect(conn_str)
        except Exception as e:
            print(f"Erreur connexion SQL Server: {e}")
            return None
    
    def connect_access(self):
        """Connexion à Access"""
        try:
            conn_str = (
                f"DRIVER={self.config['access']['driver']};"
                f"DBQ={self.config['access']['dbq']};"
            )
            return pyodbc.connect(conn_str)
        except Exception as e:
            print(f"Erreur connexion Access: {e}")
            return None
    
    def connect_target(self):
        """Connexion à la base cible (SQLite)"""
        try:
            engine = create_engine(f"sqlite:///{self.config['target']['database']}")
            return engine
        except Exception as e:
            print(f"Erreur connexion base cible: {e}")
            return None