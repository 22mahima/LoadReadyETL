#!/usr/bin/env python3
"""
LoadReady Web Interface
======================

Flask web application providing a user-friendly interface for the LoadReady ETL pipeline.
Allows file upload and displays comprehensive processing results.
"""

from flask import Flask, render_template, request, jsonify, send_file
import os
import json
import sqlite3
import pandas as pd
from datetime import datetime
from werkzeug.utils import secure_filename
import tempfile
import shutil

# Import our ETL pipeline
from loadready import LoadReadyETL

app = Flask(__name__)
app.config['SECRET_KEY'] = 'loadready-etl-pipeline-2025'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Configure upload folder
UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# Allowed file extensions
ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    """Check if file has allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """Main page with file upload interface."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_files():
    """Handle file upload and process through ETL pipeline."""
    try:
        # Check if files were uploaded
        if 'files' not in request.files:
            return jsonify({'success': False, 'error': 'No files uploaded'})
        
        files = request.files.getlist('files')
        
        if not files or all(file.filename == '' for file in files):
            return jsonify({'success': False, 'error': 'No files selected'})
        
        # Save uploaded files
        uploaded_files = []
        for file in files:
            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                # Add timestamp to avoid conflicts
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                filename = f"{timestamp}_{filename}"
                file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                file.save(file_path)
                uploaded_files.append(file_path)
            else:
                return jsonify({'success': False, 'error': f'Invalid file type. Only CSV files allowed.'})
        
        if not uploaded_files:
            return jsonify({'success': False, 'error': 'No valid CSV files found'})
        
        # Create unique database and log files for this session
        session_id = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        db_path = f'session_db_{session_id}.db'
        log_path = f'session_log_{session_id}.log'
        
        # Run ETL pipeline
        etl = LoadReadyETL(db_path=db_path, log_path=log_path, verbose=False)
        etl.run_etl(uploaded_files)
        
        # Get results from database
        results = get_etl_results(db_path)
        
        # Clean up uploaded files
        for file_path in uploaded_files:
            try:
                os.remove(file_path)
            except:
                pass
        
        return jsonify({
            'success': True,
            'session_id': session_id,
            'results': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'error': f'Processing failed: {str(e)}'})

def get_etl_results(db_path):
    """Extract ETL results from database."""
    try:
        with sqlite3.connect(db_path) as conn:
            # Get clean data
            clean_df = pd.read_sql_query('SELECT * FROM clean_data', conn)
            clean_data = clean_df.to_dict('records') if not clean_df.empty else []
            
            # Get quarantined data
            quarantine_df = pd.read_sql_query('SELECT * FROM quarantine', conn)
            quarantine_data = quarantine_df.to_dict('records') if not quarantine_df.empty else []
            
            # Calculate summary statistics
            total_clean = len(clean_data)
            total_quarantined = len(quarantine_data)
            total_processed = total_clean + total_quarantined
            success_rate = (total_clean / max(total_processed, 1)) * 100
            
            return {
                'summary': {
                    'total_processed': total_processed,
                    'clean_records': total_clean,
                    'quarantined_records': total_quarantined,
                    'success_rate': round(success_rate, 2)
                },
                'clean_data': clean_data,
                'quarantine_data': quarantine_data
            }
    except Exception as e:
        return {'error': f'Failed to read results: {str(e)}'}

@app.route('/download_report/<session_id>')
def download_report(session_id):
    """Download quality report for a session."""
    try:
        # Find the quality report file
        report_files = [f for f in os.listdir('.') if f.startswith(f'quality_report_') and session_id[:8] in f]
        
        if report_files:
            report_file = report_files[0]
            return send_file(report_file, as_attachment=True, download_name=f'loadready_report_{session_id}.csv')
        else:
            return jsonify({'error': 'Report file not found'}), 404
            
    except Exception as e:
        return jsonify({'error': f'Failed to download report: {str(e)}'}), 500

@app.route('/download_clean/<session_id>')
def download_clean_data(session_id):
    """Download clean data as CSV for a session."""
    try:
        db_path = f'session_db_{session_id}.db'
        
        if not os.path.exists(db_path):
            return jsonify({'error': 'Session data not found'}), 404
        
        with sqlite3.connect(db_path) as conn:
            clean_df = pd.read_sql_query('SELECT * FROM clean_data', conn)
            
            if clean_df.empty:
                return jsonify({'error': 'No clean data available'}), 404
            
            # Remove timestamp column for cleaner export
            if 'created_at' in clean_df.columns:
                clean_df = clean_df.drop('created_at', axis=1)
            
            # Create temporary CSV file
            temp_file = f'clean_data_{session_id}.csv'
            clean_df.to_csv(temp_file, index=False)
            
            return send_file(temp_file, as_attachment=True, download_name=f'clean_data_{session_id}.csv')
            
    except Exception as e:
        return jsonify({'error': f'Failed to download clean data: {str(e)}'}), 500

@app.route('/health')
def health_check():
    """Health check endpoint."""
    return jsonify({'status': 'healthy', 'service': 'LoadReady ETL Web Interface'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)