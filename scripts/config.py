# Configuration des bases de données
DB_CONFIG = {
    'sql_server': {
        'driver': '{SQL Server}',
        'server': 'localhost',
        'database': 'Northwind',
        'trusted_connection': 'yes'
    },
    'access': {
        'driver': '{Microsoft Access Driver (*.mdb, *.accdb)}',
        'dbq': '../data/Northwind2012(1).accdb'
    },
    'target': {
        'dialect': 'sqlite',
        'database': '../data/northwind_bi.db'
    }
}