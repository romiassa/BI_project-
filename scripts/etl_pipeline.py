import pandas as pd
from database_connectors import DatabaseConnector
from config import DB_CONFIG
import os

class ETLPipeline:
    def __init__(self):
        self.db = DatabaseConnector(DB_CONFIG)
        self.tables_metadata = {}
    
    def extract_sql_server_tables(self):
        """Extraction des tables depuis SQL Server"""
        print("🔍 Extraction des données SQL Server...")
        conn = self.db.connect_sql_server()
        if conn:
            try:
                # Liste des tables principales
                tables = ['Customers', 'Products', 'Orders', 'Order Details', 
                         'Employees', 'Suppliers', 'Categories']
                
                for table in tables:
                    query = f"SELECT * FROM {table}"
                    df = pd.read_sql(query, conn)
                    self.tables_metadata[f'sql_{table.lower()}'] = df
                    print(f"   Table {table}: {len(df)} lignes")
                
            except Exception as e:
                print(f" Erreur extraction SQL Server: {e}")
            finally:
                conn.close()
    
    def extract_access_tables(self):
        """Extraction des tables depuis Access"""
        print("🔍 Extraction des données Access...")
        conn = self.db.connect_access()
        if conn:
            try:
                # Récupération de la liste des tables
                cursor = conn.cursor()
                tables = [table.table_name for table in cursor.tables(tableType='TABLE')]
                
                for table in tables:
                    if not table.startswith('MSys'):
                        query = f"SELECT * FROM [{table}]"
                        df = pd.read_sql(query, conn)
                        self.tables_metadata[f'access_{table.lower()}'] = df
                        print(f"   Table {table}: {len(df)} lignes")
                
            except Exception as e:
                print(f"❌ Erreur extraction Access: {e}")
            finally:
                conn.close()
    
    def transform_data(self):
        """Transformation et uniformisation des données"""
        print(" Transformation des données...")
        # Ici nous allons créer la structure uniforme
        # Cette partie sera complétée après analyse des données
        
        # Exemple de transformation simple
        if 'sql_customers' in self.tables_metadata:
            df = self.tables_metadata['sql_customers']
            # Nettoyage basique
            df['CustomerID'] = df['CustomerID'].str.strip()
            print("  Transformation Customers")
    
    def load_to_target(self):
        """Chargement dans la base cible"""
        print("Chargement des données...")
        engine = self.db.connect_target()
        if engine:
            try:
                for table_name, df in self.tables_metadata.items():
                    df.to_sql(table_name, engine, if_exists='replace', index=False)
                    print(f"  Chargement {table_name}: {len(df)} lignes")
                
                print(f" Toutes les données chargées dans {DB_CONFIG['target']['database']}")
            except Exception as e:
                print(f" Erreur chargement: {e}")
    
    def run_pipeline(self):
        """Exécution complète du pipeline ETL"""
        print(" Démarrage du pipeline ETL...")
        self.extract_sql_server_tables()
        self.extract_access_tables()
        self.transform_data()
        self.load_to_target()
        print("Pipeline ETL terminé avec succès!")

if __name__ == "__main__":
    etl = ETLPipeline()
    etl.run_pipeline()