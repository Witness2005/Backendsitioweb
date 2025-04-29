# Backend mejorado para extraer datos CSV de Our World in Data y almacenarlos en PostgreSQL
import os
import requests
import pandas as pd
import logging
import psycopg2
from psycopg2 import sql
from io import StringIO
from datetime import datetime

# Configuración de logging
tk = logging.getLogger()
tk.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if not tk.handlers:
    file_handler = logging.FileHandler("birth_rate_extractor.log")
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    tk.addHandler(file_handler)
    tk.addHandler(stream_handler)
logger = logging.getLogger("birth_rate_extractor")

class CSVtoPostgresExtractor:
    def __init__(self, csv_url: str, table_name: str):
        """
        Inicializa el extractor de CSV a PostgreSQL

        Args:
            csv_url: URL del archivo CSV a descargar
            table_name: Nombre de la tabla destino en PostgreSQL
        """
        self.csv_url = csv_url
        self.table_name = table_name
        self.df = None
        self.conn = None

    def _load_env_config(self):
        # Lectura de configuración desde variables de entorno
        db_url = os.getenv("DATABASE_URL")
        if db_url:
            return { "dsn": db_url }
        return {
            "host": os.getenv("DB_HOST", "localhost"),
            "port": os.getenv("DB_PORT", "5432"),
            "database": os.getenv("DB_NAME", "postgres"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "")
        }

    def connect(self) -> bool:
        """
        Conecta con la base de datos PostgreSQL usando configuración de entorno
        """
        try:
            config = self._load_env_config()
            self.conn = psycopg2.connect(**config)
            logger.info("Conexión establecida con PostgreSQL")
            return True
        except Exception as e:
            logger.error(f"Error al conectar con PostgreSQL: {e}")
            return False

    def fetch_csv(self) -> bool:
        """
        Descarga y carga el CSV en un DataFrame de pandas
        """
        try:
            logger.info(f"Descargando CSV desde: {self.csv_url}")
            resp = requests.get(self.csv_url, timeout=30)
            resp.raise_for_status()
            self.df = pd.read_csv(StringIO(resp.text))
            rows, cols = self.df.shape
            logger.info(f"CSV cargado: {rows} filas x {cols} columnas")
            return True
        except Exception as e:
            logger.error(f"Error al obtener el CSV: {e}")
            return False

    def _sanitize_columns(self):
        # Convertir nombres de columna a snake_case y solo caracteres alfanuméricos + '_'
        clean_cols = []
        for col in self.df.columns:
            name = col.strip().lower()
            name = ''.join(c if c.isalnum() or c=='_' else '_' for c in name)
            clean_cols.append(name)
        self.df.columns = clean_cols

    def create_table(self) -> bool:
        """
        Crea la tabla si no existe, usando tipos inferidos de pandas
        """
        if self.conn is None or self.df is None:
            logger.error("No hay conexión o datos para crear tabla")
            return False
        try:
            self._sanitize_columns()
            type_map = {
                'int64': 'INTEGER',
                'float64': 'NUMERIC',
                'object': 'TEXT',
                'bool': 'BOOLEAN',
                'datetime64[ns]': 'TIMESTAMP'
            }
            cols_defs = []
            for col, dtype in self.df.dtypes.items():
                pg_type = type_map.get(str(dtype), 'TEXT')
                cols_defs.append(sql.SQL("{} {}").format(
                    sql.Identifier(col), sql.SQL(pg_type)
                ))

            create = sql.SQL("CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, {}, import_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
            stmt = create.format(
                sql.Identifier(self.table_name),
                sql.SQL(', ').join(cols_defs)
            )
            with self.conn.cursor() as cur:
                cur.execute(stmt)
            self.conn.commit()
            logger.info(f"Tabla '{self.table_name}' creada/verificada")
            return True
        except Exception as e:
            logger.error(f"Error al crear tabla: {e}")
            self.conn.rollback()
            return False

    def bulk_insert(self) -> bool:
        """
        Inserta masivamente los datos usando COPY FROM STDIN
        """
        if self.conn is None or self.df is None:
            logger.error("No hay conexión o datos para insertar")
            return False
        try:
            self._sanitize_columns()
            buffer = StringIO()
            self.df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cols = [sql.Identifier(c) for c in self.df.columns]
            copy_sql = sql.SQL("COPY {}({}) FROM STDIN WITH (FORMAT CSV)")
            copy_stmt = copy_sql.format(
                sql.Identifier(self.table_name), sql.SQL(', ').join(cols)
            )

            with self.conn.cursor() as cur:
                cur.copy_expert(copy_stmt.as_string(self.conn), buffer)
            self.conn.commit()
            logger.info(f"{len(self.df)} registros insertados en '{self.table_name}'")
            return True
        except Exception as e:
            logger.error(f"Error en inserción masiva: {e}")
            self.conn.rollback()
            return False

    def close(self):
        """Cierra la conexión con la base de datos"""
        if self.conn:
            self.conn.close()
            logger.info("Conexión cerrada")


def main():
    csv_url = os.getenv("CSV_URL", 
        "https://ourworldindata.org/grapher/crude-birth-rate.csv?v=1&csvType=full&useColumnShortNames=false")
    table = os.getenv("TARGET_TABLE", "crude_birth_rate")

    extractor = CSVtoPostgresExtractor(csv_url, table)
    if not extractor.connect():
        return
    try:
        if not extractor.fetch_csv():
            return
        if not extractor.create_table():
            return
        if not extractor.bulk_insert():
            return
        logger.info("Proceso completado con éxito")
    finally:
        extractor.close()

if __name__ == "__main__":
    main()