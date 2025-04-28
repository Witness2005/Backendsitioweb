# Backend para extraer datos CSV de Our World in Data y almacenarlos en PostgreSQL
import requests
import pandas as pd
import logging
import psycopg2
from psycopg2 import sql
from io import StringIO
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("birth_rate_extractor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("birth_rate_extractor")

class CSVtoPostgresExtractor:
    def __init__(self, csv_url: str, db_config: dict):
        """
        Inicializa el extractor de CSV a PostgreSQL
        
        Args:
            csv_url: URL del archivo CSV a descargar
            db_config: Configuración de la base de datos PostgreSQL
        """
        self.csv_url = csv_url
        self.db_config = db_config
        self.db_connection = None
        self.df = None
    
    def connect_to_database(self):
        """
        Establece conexión con la base de datos PostgreSQL
        
        Returns:
            bool: True si la conexión fue exitosa, False en caso contrario
        """
        try:
            self.db_connection = psycopg2.connect(
                host=self.db_config.get("host"),
                database=self.db_config.get("database"),
                user=self.db_config.get("user"),
                password=self.db_config.get("password"),
                port=self.db_config.get("port", 5432)
            )
            logger.info("Conexión establecida con PostgreSQL")
            return True
                
        except Exception as e:
            logger.error(f"Error al conectar con PostgreSQL: {str(e)}")
            return False
    
    def fetch_csv_data(self):
        """
        Descarga y procesa los datos CSV de la URL proporcionada
        
        Returns:
            bool: True si la descarga fue exitosa, False en caso contrario
        """
        try:
            logger.info(f"Descargando CSV desde: {self.csv_url}")
            response = requests.get(self.csv_url)
            response.raise_for_status()
            
            # Cargar CSV en un DataFrame de pandas
            csv_content = StringIO(response.text)
            self.df = pd.read_csv(csv_content)
            
            # Registrar información sobre los datos obtenidos
            rows, cols = self.df.shape
            logger.info(f"CSV descargado y procesado: {rows} filas, {cols} columnas")
            logger.info(f"Columnas: {', '.join(self.df.columns.tolist())}")
            
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al descargar el CSV: {str(e)}")
            return False
        except pd.errors.ParserError as e:
            logger.error(f"Error al procesar el CSV: {str(e)}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}")
            return False
    
    def create_table_if_not_exists(self, table_name: str):
        """
        Crea la tabla en PostgreSQL si no existe
        
        Args:
            table_name: Nombre de la tabla a crear
            
        Returns:
            bool: True si la operación fue exitosa, False en caso contrario
        """
        if not self.db_connection or not self.df is not None:
            logger.error("No hay conexión a la base de datos o no se han cargado datos CSV")
            return False
        
        try:
            cursor = self.db_connection.cursor()
            
            # Generar la definición de columnas basada en el DataFrame
            columns_definitions = []
            
            # Mapeo de tipos de datos de pandas a PostgreSQL
            type_mapping = {
                'int64': 'INTEGER',
                'float64': 'NUMERIC',
                'object': 'TEXT',
                'datetime64[ns]': 'TIMESTAMP',
                'bool': 'BOOLEAN'
            }
            
            # Preparar definiciones de columnas
            for col in self.df.columns:
                # Sanitizar nombre de columna para SQL
                col_name = col.replace(' ', '_').lower()
                # Obtener tipo de datos de pandas
                pandas_type = str(self.df[col].dtype)
                # Convertir a tipo PostgreSQL
                pg_type = type_mapping.get(pandas_type, 'TEXT')
                
                columns_definitions.append(f"\"{col_name}\" {pg_type}")
            
            # Crear la tabla
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                {', '.join(columns_definitions)},
                import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            cursor.execute(create_table_query)
            self.db_connection.commit()
            cursor.close()
            
            logger.info(f"Tabla '{table_name}' creada o verificada en PostgreSQL")
            return True
            
        except Exception as e:
            logger.error(f"Error al crear la tabla en PostgreSQL: {str(e)}")
            if self.db_connection:
                self.db_connection.rollback()
            return False
    
    def save_to_database(self, table_name: str):
        """
        Guarda los datos del DataFrame en la tabla PostgreSQL
        
        Args:
            table_name: Nombre de la tabla donde guardar los datos
            
        Returns:
            bool: True si la operación fue exitosa, False en caso contrario
        """
        if not self.db_connection or self.df is None:
            logger.error("No hay conexión a la base de datos o no se han cargado datos")
            return False
        
        try:
            # Sanitizar nombres de columnas
            self.df.columns = [col.replace(' ', '_').lower() for col in self.df.columns]
            
            # Convertir DataFrame a una lista de tuplas para inserción
            columns = self.df.columns.tolist()
            values = [tuple(row) for row in self.df.to_numpy()]
            
            cursor = self.db_connection.cursor()
            
            # Construir la consulta de inserción
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join([f'"{col}"' for col in columns])
            
            insert_query = f"""
            INSERT INTO {table_name} ({columns_str})
            VALUES ({placeholders})
            """
            
            # Ejecutar la inserción por lotes
            batch_size = 1000
            for i in range(0, len(values), batch_size):
                batch = values[i:i+batch_size]
                cursor.executemany(insert_query, batch)
                self.db_connection.commit()
                logger.info(f"Lote {i//batch_size + 1} insertado: {len(batch)} registros")
            
            cursor.close()
            logger.info(f"Total de {len(values)} registros guardados en la tabla '{table_name}'")
            return True
            
        except Exception as e:
            logger.error(f"Error al guardar datos en PostgreSQL: {str(e)}")
            if self.db_connection:
                self.db_connection.rollback()
            return False
    
    def close_connection(self):
        """
        Cierra la conexión con la base de datos
        """
        if self.db_connection:
            self.db_connection.close()
            logger.info("Conexión cerrada con PostgreSQL")

# Ejemplo de uso con tus credenciales específicas
def main():
    # URL de la API CSV de Our World in Data - tasas de natalidad
    csv_url = "https://ourworldindata.org/grapher/crude-birth-rate.csv?v=1&csvType=full&useColumnShortNames=false"
    
    # Configuración de la base de datos PostgreSQL
    db_config = {
        "type": "postgresql",
        "host": "localhost",
        "port": "5432",
        "database": "mi_base_datos_transmision",
        "user": "postgres",
        "password": "12345",
    }
    
    # Nombre de la tabla donde se guardarán los datos
    table_name = "crude_birth_rate"
    
    # Inicializar el extractor
    extractor = CSVtoPostgresExtractor(csv_url, db_config)
    
    # Conectar a la base de datos
    if not extractor.connect_to_database():
        logger.error("No se pudo conectar a la base de datos PostgreSQL")
        return
    
    try:
        # Descargar y procesar datos CSV
        if not extractor.fetch_csv_data():
            logger.error("No se pudieron obtener los datos CSV")
            return
        
        # Crear la tabla si no existe
        if not extractor.create_table_if_not_exists(table_name):
            logger.error("No se pudo crear la tabla en PostgreSQL")
            return
        
        # Guardar datos en PostgreSQL
        if extractor.save_to_database(table_name):
            logger.info("Proceso de extracción y carga completado con éxito")
        else:
            logger.error("Error al guardar los datos en PostgreSQL")
            
    finally:
        # Cerrar la conexión
        extractor.close_connection()

if __name__ == "__main__":
    main()