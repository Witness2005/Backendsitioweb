from flask import Flask, jsonify, render_template, send_from_directory, request
import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

app = Flask(__name__)

def get_db_connection():
    """Establece conexión con la base de datos PostgreSQL"""
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return psycopg2.connect(db_url)
    
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        database=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "")
    )

@app.route('/')
def index_page():
    """Sirve la página principal HTML"""
    return send_from_directory('static', 'index.html')

@app.route('/api')
def api_index():
    """Página de inicio de la API con información básica"""
    return jsonify({
        "name": "API de Datos de Natalidad",
        "description": "API para acceder a datos de tasa de natalidad mundial",
        "endpoints": [
            "/api/birth-rates",
            "/api/birth-rates/countries",
            "/api/birth-rates/country/<code>",
            "/api/birth-rates/years"
        ]
    })

@app.route('/api/birth-rates')
def get_birth_rates():
    """Retorna datos de tasas de natalidad con paginación"""
    page = request.args.get('page', 1, type=int)
    limit = request.args.get('limit', 100, type=int)
    offset = (page - 1) * limit
    
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Obtener datos con paginación
            cur.execute("SELECT * FROM crude_birth_rate ORDER BY entity, year LIMIT %s OFFSET %s", 
                       (limit, offset))
            results = cur.fetchall()
            
            # Obtener el total de registros para la paginación
            cur.execute("SELECT COUNT(*) FROM crude_birth_rate")
            total = cur.fetchone()['count']
            
            return jsonify({
                "page": page,
                "limit": limit,
                "total": total,
                "total_pages": (total + limit - 1) // limit,
                "data": results
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/birth-rates/countries')
def get_countries():
    """Retorna lista de países disponibles"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Obtener países únicos con sus códigos
            cur.execute("""
                SELECT DISTINCT entity, code 
                FROM crude_birth_rate 
                WHERE code IS NOT NULL 
                ORDER BY entity
            """)
            results = cur.fetchall()
            return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/birth-rates/country/<code>')
def get_country_data(code):
    """Retorna datos de un país específico por código"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM crude_birth_rate 
                WHERE code = %s 
                ORDER BY year
            """, (code,))
            results = cur.fetchall()
            if not results:
                return jsonify({"error": "País no encontrado"}), 404
            
            # Obtener información general del país
            country_name = results[0]['entity']
            year_min = results[0]['year']
            year_max = results[-1]['year']
            
            return jsonify({
                "country": country_name,
                "code": code,
                "year_range": f"{year_min}-{year_max}",
                "data_points": len(results),
                "data": results
            })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/birth-rates/years')
def get_years_range():
    """Retorna el rango de años disponibles en la base de datos"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT MIN(year) as min_year, MAX(year) as max_year FROM crude_birth_rate")
            result = cur.fetchone()
            return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/birth-rates/summary')
def get_summary_statistics():
    """Retorna estadísticas resumidas de tasas de natalidad por año"""
    conn = get_db_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT 
                    year,
                    AVG(crude_birth_rate) as average_rate,
                    MIN(crude_birth_rate) as min_rate,
                    MAX(crude_birth_rate) as max_rate,
                    COUNT(*) as country_count
                FROM crude_birth_rate
                GROUP BY year
                ORDER BY year
            """)
            results = cur.fetchall()
            return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/health')
def health_check():
    """Endpoint para verificar el estado de la API"""
    try:
        conn = get_db_connection()
        conn.close()
        return jsonify({"status": "healthy", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "unhealthy", "database": "disconnected", "error": str(e)}), 500

@app.errorhandler(404)
def page_not_found(e):
    """Manejador para rutas no encontradas"""
    return jsonify({"error": "Ruta no encontrada"}), 404

@app.errorhandler(500)
def server_error(e):
    """Manejador para errores del servidor"""
    return jsonify({"error": "Error interno del servidor", "details": str(e)}), 500

if __name__ == '__main__':
    port = int(os.getenv('PORT', 5000))
    debug = os.getenv('FLASK_ENV') == 'development'
    app.run(host='0.0.0.0', port=port, debug=debug)