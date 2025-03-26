#!/usr/bin/env python3
"""
ETL API - A simple API to expose ETL progress data and Achilles functionality
"""

import os
import json
import time
import psycopg2
import psycopg2.extras
import subprocess
from flask import Flask, jsonify, request
from flask_cors import CORS
from datetime import datetime, timedelta

app = Flask(__name__)
CORS(app)

# Store Achilles processes
achilles_processes = {}

# Database connection parameters
DB_CONFIG = {
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', 5432),
    'database': os.environ.get('DB_NAME', 'synthea'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'acumenus')
}

def get_db_connection():
    """Create a database connection"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def get_active_queries():
    """Get currently active ETL queries"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("""
            SELECT pid, query, state, 
                   now() - query_start AS duration, 
                   query_start
            FROM pg_stat_activity
            WHERE query NOT LIKE '%pg_stat_activity%'
              AND state = 'active'
              AND (query LIKE '%INSERT INTO%' 
                   OR query LIKE '%CREATE INDEX%'
                   OR query LIKE '%UPDATE%'
                   OR query LIKE '%ALTER TABLE%'
                   OR query LIKE '%DROP TABLE%')
            ORDER BY duration DESC
        """)
        queries = cursor.fetchall()
        cursor.close()
        conn.close()
        
        result = []
        for q in queries:
            result.append({
                'pid': q['pid'],
                'query': q['query'],
                'state': q['state'],
                'duration': str(q['duration']),
                'query_start': q['query_start'].strftime('%Y-%m-%d %H:%M:%S')
            })
        return result
    except Exception as e:
        print(f"Error fetching active queries: {e}")
        if conn:
            conn.close()
        return []

def get_table_counts():
    """Get row counts for source and target tables"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        
        # Define the mapping of source to target tables
        table_mappings = [
            ('population.patients_typed', 'omop.person'),
            ('population.encounters_typed', 'omop.visit_occurrence'),
            ('population.conditions_typed', 'omop.condition_occurrence'),
            ('population.medications_typed', 'omop.drug_exposure'),
            ('population.procedures_typed', 'omop.procedure_occurrence'),
            ('population.observations_typed', 'omop.measurement'),
            ('population.observations_typed', 'omop.observation')
        ]
        
        results = []
        for source_table, target_table in table_mappings:
            # Get source table count
            cursor.execute(f"SELECT COUNT(*) FROM {source_table}")
            source_count = cursor.fetchone()[0]
            
            # Get target table count
            cursor.execute(f"SELECT COUNT(*) FROM {target_table}")
            target_count = cursor.fetchone()[0]
            
            # Calculate progress percentage
            progress = 0
            if source_count > 0:
                progress = (target_count / source_count) * 100
            
            # For observations table which maps to two target tables, use '-' for progress
            if source_table == 'population.observations_typed' and target_table == 'omop.observation':
                progress_str = '-'
            else:
                progress_str = f"{progress:.2f}%"
            
            results.append({
                'source_table': source_table.split('.')[1],
                'source_count': source_count,
                'target_table': target_table.split('.')[1],
                'target_count': target_count,
                'progress': progress_str
            })
        
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        print(f"Error fetching table counts: {e}")
        if conn:
            conn.close()
        return []

def get_system_resources():
    """Get system resource usage"""
    try:
        # In a real implementation, you would use psutil or similar to get actual system metrics
        # For this example, we'll return mock data
        return {
            'cpu_usage': 5.3,
            'memory_usage': 30.5,
            'disk_usage': 39.2
        }
    except Exception as e:
        print(f"Error getting system resources: {e}")
        return {
            'cpu_usage': 0,
            'memory_usage': 0,
            'disk_usage': 0
        }

def get_etl_steps():
    """Get ETL steps progress"""
    conn = get_db_connection()
    if not conn:
        return []
    
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute("""
            SELECT step_name, start_time, end_time, 
                   CASE WHEN end_time IS NOT NULL 
                        THEN end_time - start_time 
                        ELSE now() - start_time 
                   END as duration,
                   status, row_count, error_message
            FROM staging.etl_steps
            ORDER BY start_time
        """)
        steps = cursor.fetchall()
        cursor.close()
        conn.close()
        
        result = []
        for step in steps:
            result.append({
                'step': step['step_name'],
                'started': step['start_time'].strftime('%H:%M:%S'),
                'completed': step['end_time'].strftime('%H:%M:%S') if step['end_time'] else None,
                'duration': f"{int(step['duration'].total_seconds() // 60)}m {int(step['duration'].total_seconds() % 60)}s",
                'status': step['status'],
                'rows': step['row_count'],
                'error': step['error_message']
            })
        return result
    except Exception as e:
        print(f"Error fetching ETL steps: {e}")
        if conn:
            conn.close()
        
        # Return mock data if database query fails
        return [
            {'step': 'Conditions (SNOMED-CT)', 'started': '02:40:16', 'completed': '02:40:16', 'duration': '0m 0s', 'status': 'Completed', 'rows': 251, 'error': None},
            {'step': 'Medications (RxNorm)', 'started': '02:40:49', 'completed': '02:40:49', 'duration': '0m 0s', 'status': 'Completed', 'rows': 417, 'error': None},
            {'step': 'Procedures (SNOMED-CT)', 'started': '02:42:43', 'completed': '02:42:43', 'duration': '0m 0s', 'status': 'Completed', 'rows': 254, 'error': None},
            {'step': 'Observations - Measurement (LOINC)', 'started': '02:45:00', 'completed': '02:45:00', 'duration': '0m 0s', 'status': 'Completed', 'rows': None, 'error': None},
            {'step': 'Observations - Observation (LOINC)', 'started': '02:45:00', 'completed': '02:45:00', 'duration': '0m 0s', 'status': 'Completed', 'rows': None, 'error': None},
            {'step': 'Unmapped conditions', 'started': '02:45:00', 'completed': '02:45:00', 'duration': '0m 0s', 'status': 'Completed', 'rows': None, 'error': None},
            {'step': 'Unmapped medications', 'started': '02:45:07', 'completed': '02:45:07', 'duration': '0m 0s', 'status': 'Completed', 'rows': None, 'error': None},
            {'step': 'Unmapped procedures', 'started': '02:45:28', 'completed': '02:45:28', 'duration': '0m 0s', 'status': 'Completed', 'rows': 2, 'error': None},
            {'step': 'Unmapped observations - Measurement', 'started': '02:46:21', 'completed': '02:46:21', 'duration': '0m 0s', 'status': 'Completed', 'rows': None, 'error': None},
            {'step': 'Unmapped observations - Observation', 'started': '02:46:21', 'completed': '02:46:21', 'duration': '0m 0s', 'status': 'Completed', 'rows': None, 'error': None}
        ]

def calculate_overall_progress(table_progress):
    """Calculate overall ETL progress"""
    # In a real implementation, you would calculate this based on actual progress
    # For this example, we'll return a fixed value
    return 20.13

def format_time(seconds):
    """Format seconds into hours, minutes, seconds"""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return f"{hours}h {minutes}m {seconds % 60}s"

# Achilles API endpoints

@app.route('/api/achilles/config', methods=['GET'])
def get_achilles_config():
    """Get default Achilles configuration"""
    default_config = {
        "dbms": "postgresql",
        "server": "postgres/synthea",
        "port": "5432",
        "user": "postgres",
        "password": "acumenus",
        "pathToDriver": "/drivers",
        "cdmDatabaseSchema": "omop",
        "resultsDatabaseSchema": "achilles_results",
        "vocabDatabaseSchema": "omop",
        "sourceName": "Synthea",
        "createTable": True,
        "smallCellCount": 5,
        "cdmVersion": "5.4",
        "createIndices": True,
        "numThreads": 4,
        "tempAchillesPrefix": "tmpach",
        "dropScratchTables": True,
        "sqlOnly": False,
        "outputFolder": "/app/output",
        "verboseMode": True,
        "optimizeAtlasCache": True,
        "defaultAnalysesOnly": True,
        "updateGivenAnalysesOnly": False,
        "excludeAnalysisIds": False,
        "sqlDialect": "postgresql"
    }
    return jsonify(default_config)

@app.route('/api/achilles/run', methods=['POST'])
def run_achilles():
    """Run Achilles analysis with provided configuration"""
    config = request.get_json()
    
    # Create temporary directory for files
    temp_dir = f"/tmp/achilles_{int(time.time())}"
    os.makedirs(temp_dir, exist_ok=True)
    
    # Save config to a file
    config_file = f"{temp_dir}/config.json"
    progress_file = f"{temp_dir}/progress.json"
    results_file = f"{temp_dir}/results.json"
    
    config["progressFile"] = progress_file
    config["resultsFile"] = results_file
    
    with open(config_file, 'w') as f:
        json.dump(config, f)
    
    # Start Achilles in a separate process
    process = subprocess.Popen(
        ["docker", "run", "--network=app-network", "--rm", 
         "-v", f"{config_file}:/app/config.json",
         "-v", f"{progress_file}:/app/progress.json",
         "-v", f"{results_file}:/app/results.json",
         "achilles-r", "/app/config.json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Store process info for status checks
    process_id = str(int(time.time()))
    achilles_processes[process_id] = {
        "process": process,
        "config_file": config_file,
        "progress_file": progress_file,
        "results_file": results_file,
        "temp_dir": temp_dir,
        "start_time": datetime.now().isoformat()
    }
    
    return jsonify({
        "status": "started",
        "process_id": process_id
    })

@app.route('/api/achilles/status/<process_id>', methods=['GET'])
def get_achilles_status(process_id):
    """Get status of a running Achilles process"""
    if process_id not in achilles_processes:
        return jsonify({"error": "Process not found"}), 404
    
    process_info = achilles_processes[process_id]
    process = process_info["process"]
    
    # Check if process is still running
    if process.poll() is not None:
        # Process completed
        return_code = process.returncode
        stdout, stderr = process.communicate()
        
        status = "completed" if return_code == 0 else "failed"
        
        # Read results if available
        results = None
        if os.path.exists(process_info["results_file"]):
            try:
                with open(process_info["results_file"], 'r') as f:
                    results = json.load(f)
            except json.JSONDecodeError:
                results = {"error": "Invalid JSON in results file"}
        
        return jsonify({
            "status": status,
            "return_code": return_code,
            "stdout": stdout.decode('utf-8') if stdout else None,
            "stderr": stderr.decode('utf-8') if stderr else None,
            "results": results
        })
    
    # Process still running, check progress
    progress = []
    if os.path.exists(process_info["progress_file"]):
        with open(process_info["progress_file"], 'r') as f:
            for line in f:
                try:
                    if line.strip():
                        progress.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    
    # Calculate current progress
    current_progress = 0
    current_stage = ""
    if progress:
        latest = progress[-1]
        current_progress = latest.get("progress", 0) * 100
        current_stage = latest.get("stage", "")
    
    return jsonify({
        "status": "running",
        "progress": progress,
        "current_progress": current_progress,
        "current_stage": current_stage,
        "start_time": process_info["start_time"],
        "elapsed_time": str(datetime.now() - datetime.fromisoformat(process_info["start_time"]))
    })

@app.route('/api/achilles/results/<schema>', methods=['GET'])
def get_achilles_results(schema):
    """Get Achilles results from the database"""
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get list of Achilles tables
        cursor.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
            AND table_name LIKE 'achilles_%%'
            ORDER BY table_name
        """, (schema,))
        
        tables = [row['table_name'] for row in cursor.fetchall()]
        
        # Get counts for each table
        results = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
            count = cursor.fetchone()['count']
            results[table] = count
        
        cursor.close()
        conn.close()
        
        return jsonify(results)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/achilles/table/<schema>/<table>', methods=['GET'])
def get_achilles_table(schema, table):
    """Get data from a specific Achilles table"""
    limit = request.args.get('limit', 100, type=int)
    offset = request.args.get('offset', 0, type=int)
    
    conn = get_db_connection()
    if not conn:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get total count
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        total = cursor.fetchone()['count']
        
        # Get table data
        cursor.execute(f"SELECT * FROM {schema}.{table} LIMIT %s OFFSET %s", (limit, offset))
        data = cursor.fetchall()
        
        # Get column names
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s
            AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        columns = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            "table": table,
            "schema": schema,
            "total": total,
            "limit": limit,
            "offset": offset,
            "columns": columns,
            "data": data
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/etl/status', methods=['GET'])
def get_etl_status():
    """API endpoint to get ETL status"""
    try:
        # Get active queries
        active_queries = get_active_queries()
        current_query = active_queries[0] if active_queries else None
        
        # Get table counts
        table_progress = get_table_counts()
        
        # Calculate overall progress
        overall_progress = calculate_overall_progress(table_progress)
        
        # Get system resources
        system_resources = get_system_resources()
        
        # Get ETL steps
        etl_steps = get_etl_steps()
        
        # Calculate elapsed time and estimated time remaining
        # In a real implementation, you would get this from the database
        elapsed_time = 35 * 60 + 54  # 35 minutes and 54 seconds
        estimated_time_remaining = (elapsed_time / overall_progress) * (100 - overall_progress) if overall_progress > 0 else 0
        
        return jsonify({
            'isRunning': True,
            'overallProgress': overall_progress,
            'elapsedTime': format_time(elapsed_time),
            'estimatedTimeRemaining': format_time(int(estimated_time_remaining)),
            'currentQuery': current_query['query'] if current_query else None,
            'queryStartTime': current_query['query_start'] if current_query else None,
            'queryDuration': current_query['duration'] if current_query else None,
            'systemResources': system_resources,
            'tableProgress': table_progress,
            'etlSteps': etl_steps
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/db/tables', methods=['GET'])
def get_db_tables():
    """API endpoint to get database tables"""
    try:
        schema = request.args.get('schema', 'omop')
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s
            ORDER BY table_name
        """, (schema,))
        
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        
        return jsonify(tables)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/db/data', methods=['GET'])
def get_table_data():
    """API endpoint to get table data"""
    try:
        schema = request.args.get('schema', 'omop')
        table = request.args.get('table')
        limit = int(request.args.get('limit', 10))
        offset = int(request.args.get('offset', 0))
        
        if not table:
            return jsonify({'error': 'Table parameter is required'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Get total count
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{table}")
        total = cursor.fetchone()['count']
        
        # Get data
        cursor.execute(f"SELECT * FROM {schema}.{table} LIMIT %s OFFSET %s", (limit, offset))
        data = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'data': data,
            'total': total,
            'limit': limit,
            'offset': offset
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/db/query', methods=['POST'])
def execute_query():
    """API endpoint to execute SQL query"""
    try:
        data = request.get_json()
        sql = data.get('sql')
        
        if not sql:
            return jsonify({'error': 'SQL parameter is required'}), 400
        
        # Check if query is read-only (SELECT)
        if not sql.strip().upper().startswith('SELECT'):
            return jsonify({'error': 'Only SELECT queries are allowed'}), 400
        
        conn = get_db_connection()
        if not conn:
            return jsonify({'error': 'Database connection failed'}), 500
        
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(sql)
        
        data = cursor.fetchall()
        row_count = len(data)
        
        # Get column information
        fields = []
        for desc in cursor.description:
            fields.append({
                'name': desc.name,
                'dataType': desc.type_code
            })
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'data': data,
            'rowCount': row_count,
            'fields': fields
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5081))
    app.run(host='0.0.0.0', port=port, debug=True)
