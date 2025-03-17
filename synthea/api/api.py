from flask import Flask, request, jsonify
import subprocess
import os
import json
import time
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

@app.route('/api/synthea/generate', methods=['POST'])
def generate_data():
    """Generate Synthea data with the provided parameters."""
    params = request.json
    
    # Validate parameters
    if not params or 'population' not in params:
        return jsonify({'error': 'Population parameter is required'}), 400
    
    # Build environment variables for Docker
    env = {
        'POPULATION': str(params.get('population', 1000)),
        'SEED': str(params.get('seed', 1)),
        'STATE': params.get('state', 'Massachusetts'),
        'CITY': params.get('city', 'Bedford'),
        'GENDER': params.get('gender', ''),
        'AGE': params.get('age', ''),
        'MODULE': params.get('module', '')
    }
    
    # Store the current job
    job_id = str(int(time.time()))
    job_info = {
        'id': job_id,
        'status': 'running',
        'params': params,
        'start_time': time.time(),
        'end_time': None
    }
    
    os.makedirs('/tmp/synthea_jobs', exist_ok=True)
    with open(f'/tmp/synthea_jobs/job_{job_id}.json', 'w') as f:
        json.dump(job_info, f)
    
    logger.info(f"Starting Synthea job {job_id} with parameters: {params}")
    
    # Run Synthea in Docker
    try:
        # Start a background process to monitor the Synthea container
        monitor_process = subprocess.Popen([
            'python', '-c', 
            f'''
import os
import json
import time
import subprocess

# Run Synthea
process = subprocess.Popen(
    ['docker-compose', 'run', '--rm', 'synthea'],
    env={{**os.environ, **{json.dumps(env)}}}
)

# Wait for process to complete
process.wait()

# Update job status
with open('/tmp/synthea_jobs/job_{job_id}.json', 'r') as f:
    job_info = json.load(f)

job_info['status'] = 'completed' if process.returncode == 0 else 'failed'
job_info['end_time'] = time.time()
if process.returncode != 0:
    job_info['error'] = f'Synthea process exited with code {{process.returncode}}'

with open('/tmp/synthea_jobs/job_{job_id}.json', 'w') as f:
    json.dump(job_info, f)
            '''
        ])
        
        return jsonify({'job_id': job_id, 'status': 'started'})
    except Exception as e:
        logger.error(f"Error starting Synthea job: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/synthea/status/<job_id>', methods=['GET'])
def get_status(job_id):
    """Get the status of a Synthea data generation job."""
    try:
        with open(f'/tmp/synthea_jobs/job_{job_id}.json', 'r') as f:
            job_info = json.load(f)
        
        # Check if the output directory has a .complete file
        if job_info['status'] == 'running':
            if os.path.exists('/output/.complete'):
                job_info['status'] = 'completed'
                job_info['end_time'] = time.time()
                
                with open(f'/tmp/synthea_jobs/job_{job_id}.json', 'w') as f:
                    json.dump(job_info, f)
        
        return jsonify(job_info)
    except FileNotFoundError:
        return jsonify({'error': 'Job not found'}), 404

@app.route('/api/synthea/jobs', methods=['GET'])
def list_jobs():
    """List all Synthea data generation jobs."""
    jobs = []
    
    try:
        for filename in os.listdir('/tmp/synthea_jobs'):
            if filename.startswith('job_') and filename.endswith('.json'):
                with open(f'/tmp/synthea_jobs/{filename}', 'r') as f:
                    job_info = json.load(f)
                jobs.append(job_info)
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        return jsonify({'error': str(e)}), 500
    
    return jsonify({'jobs': jobs})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5082)
