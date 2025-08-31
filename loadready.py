#!/usr/bin/env python3
"""
LoadReady - Production-Ready Python ETL Pipeline
===============================================

A comprehensive ETL pipeline for processing CSV files with schema validation,
data quality reporting, and SQLite storage.

Features:
- Extract data from multiple CSV files
- Schema validation with quarantine system
- Data transformation and cleaning
- Data quality reporting
- Load to SQLite database
- Comprehensive logging

Usage:
    python loadready.py file1.csv file2.csv --verbose

Author: LoadReady ETL Pipeline
Date: August 31, 2025
"""

import pandas as pd
import sqlite3
import logging
import argparse
import os
import sys
from typing import Dict, List, Tuple, Any, Optional
from datetime import datetime
import traceback


class LoadReadyETL:
    """
    Main ETL pipeline class for LoadReady project.
    
    This class orchestrates the entire ETL process including extraction,
    validation, transformation, quality reporting, and loading.
    """
    
    def __init__(self, db_path: str = "loadready.db", log_path: str = "loadready.log", verbose: bool = False):
        """
        Initialize the ETL pipeline.
        
        Args:
            db_path (str): Path to SQLite database file
            log_path (str): Path to log file
            verbose (bool): Whether to print logs to console
        """
        self.db_path = db_path
        self.log_path = log_path
        self.verbose = verbose
        
        # Expected schema definition
        self.expected_schema = {
            "id": int,
            "name": str,
            "age": int,
            "salary": float,
            "department": str
        }
        
        # Quality metrics tracking
        self.quality_metrics = {
            "total_rows": 0,
            "valid_rows": 0,
            "invalid_rows": 0,
            "duplicates_removed": 0,
            "missing_values": {},
            "schema_violations": 0,
            "files_processed": 0,
            "files_failed": 0
        }
        
        # Setup logging
        self._setup_logging()
        
        # Initialize database
        self._initialize_database()
    
    def _setup_logging(self):
        """
        Configure logging for the ETL pipeline.
        
        Sets up both file and console logging based on verbose flag.
        """
        # Create logger
        self.logger = logging.getLogger('LoadReady')
        self.logger.setLevel(logging.DEBUG)
        
        # Clear any existing handlers
        self.logger.handlers.clear()
        
        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # File handler
        try:
            file_handler = logging.FileHandler(self.log_path, mode='a')
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        except Exception as e:
            print(f"Warning: Could not setup file logging: {e}")
        
        # Console handler (if verbose)
        if self.verbose:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
    
    def _initialize_database(self):
        """
        Initialize SQLite database and create necessary tables.
        
        Creates both the main data table and quarantine table for invalid rows.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Create main data table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS clean_data (
                        id INTEGER PRIMARY KEY,
                        name TEXT NOT NULL,
                        age INTEGER NOT NULL,
                        salary REAL NOT NULL,
                        department TEXT NOT NULL,
                        yearly_salary REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create quarantine table for invalid rows
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS quarantine (
                        row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source_file TEXT,
                        raw_data TEXT,
                        error_reason TEXT,
                        error_column TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                conn.commit()
                self.logger.info("Database initialized successfully")
                
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    def log_step(self, step_name: str, message: str, level: str = "INFO"):
        """
        Log ETL step with structured format.
        
        Args:
            step_name (str): Name of the ETL step
            message (str): Log message
            level (str): Log level (INFO, WARNING, ERROR)
        """
        formatted_message = f"[{step_name}] {message}"
        
        if level.upper() == "ERROR":
            self.logger.error(formatted_message)
        elif level.upper() == "WARNING":
            self.logger.warning(formatted_message)
        else:
            self.logger.info(formatted_message)
    
    def extract(self, file_paths: List[str]) -> List[pd.DataFrame]:
        """
        Extract data from multiple CSV files.
        
        Args:
            file_paths (List[str]): List of CSV file paths
            
        Returns:
            List[pd.DataFrame]: List of DataFrames from successfully read files
        """
        self.log_step("EXTRACT", f"Starting extraction from {len(file_paths)} files")
        extracted_dataframes = []
        
        for file_path in file_paths:
            try:
                # Check if file exists
                if not os.path.exists(file_path):
                    self.log_step("EXTRACT", f"File not found: {file_path}", "ERROR")
                    self.quality_metrics["files_failed"] += 1
                    continue
                
                # Check if file is empty
                if os.path.getsize(file_path) == 0:
                    self.log_step("EXTRACT", f"File is empty: {file_path}", "WARNING")
                    self.quality_metrics["files_failed"] += 1
                    continue
                
                # Read CSV file
                df = pd.read_csv(file_path)
                
                if df.empty:
                    self.log_step("EXTRACT", f"No data in file: {file_path}", "WARNING")
                    self.quality_metrics["files_failed"] += 1
                    continue
                
                # Add source file column for tracking
                df['_source_file'] = file_path
                extracted_dataframes.append(df)
                
                self.log_step("EXTRACT", f"Successfully extracted {len(df)} rows from {file_path}")
                self.quality_metrics["files_processed"] += 1
                
            except pd.errors.EmptyDataError:
                self.log_step("EXTRACT", f"Empty data error in file: {file_path}", "ERROR")
                self.quality_metrics["files_failed"] += 1
            except pd.errors.ParserError as e:
                self.log_step("EXTRACT", f"Parser error in file {file_path}: {e}", "ERROR")
                self.quality_metrics["files_failed"] += 1
            except Exception as e:
                self.log_step("EXTRACT", f"Unexpected error reading {file_path}: {e}", "ERROR")
                self.quality_metrics["files_failed"] += 1
        
        total_rows = sum(len(df) for df in extracted_dataframes)
        self.quality_metrics["total_rows"] = total_rows
        self.log_step("EXTRACT", f"Extraction completed. Total rows: {total_rows}")
        
        return extracted_dataframes
    
    def validate_schema(self, dataframes: List[pd.DataFrame]) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        Validate data against expected schema and quarantine invalid rows.
        
        Args:
            dataframes (List[pd.DataFrame]): List of DataFrames to validate
            
        Returns:
            Tuple[pd.DataFrame, List[Dict]]: Valid data and list of invalid row records
        """
        self.log_step("VALIDATE", "Starting schema validation")
        valid_rows = []
        invalid_rows = []
        
        # Combine all dataframes
        if not dataframes:
            self.log_step("VALIDATE", "No data to validate", "WARNING")
            return pd.DataFrame(), []
        
        combined_df = pd.concat(dataframes, ignore_index=True)
        
        for index, row in combined_df.iterrows():
            row_valid = True
            error_reasons = []
            
            # Check if all required columns exist
            schema_columns = set(self.expected_schema.keys())
            row_columns = set(row.index)
            missing_columns = schema_columns - row_columns
            if missing_columns:
                row_valid = False
                error_reasons.append(f"Missing columns: {missing_columns}")
            
            # Validate each column against schema
            for column, expected_type in self.expected_schema.items():
                if column in row.index:
                    value = row[column]
                    
                    # Check for null/missing values
                    if pd.isna(value) or (isinstance(value, str) and value.strip() == ''):
                        row_valid = False
                        error_reasons.append(f"Missing value in column '{column}'")
                        continue
                    
                    # Type validation
                    try:
                        if expected_type == int:
                            int(value)
                        elif expected_type == float:
                            float(value)
                        elif expected_type == str:
                            str(value)
                    except (ValueError, TypeError):
                        row_valid = False
                        error_reasons.append(f"Invalid type in column '{column}': expected {expected_type.__name__}, got {type(value).__name__}")
            
            if row_valid:
                valid_rows.append(row.to_dict())
            else:
                invalid_record = {
                    "source_file": row.get('_source_file', 'unknown'),
                    "raw_data": row.to_json(),
                    "error_reason": "; ".join(error_reasons),
                    "error_column": "multiple" if len(error_reasons) > 1 else error_reasons[0].split("'")[1] if "'" in error_reasons[0] else "unknown"
                }
                invalid_rows.append(invalid_record)
                self.quality_metrics["schema_violations"] += 1
        
        valid_df = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame()
        self.quality_metrics["valid_rows"] = len(valid_df)
        self.quality_metrics["invalid_rows"] = len(invalid_rows)
        
        self.log_step("VALIDATE", f"Schema validation completed. Valid: {len(valid_df)}, Invalid: {len(invalid_rows)}")
        
        return valid_df, invalid_rows
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform and clean the data.
        
        Args:
            df (pd.DataFrame): DataFrame to transform
            
        Returns:
            pd.DataFrame: Transformed DataFrame
        """
        self.log_step("TRANSFORM", "Starting data transformation")
        
        if df.empty:
            self.log_step("TRANSFORM", "No data to transform", "WARNING")
            return df
        
        original_count = len(df)
        
        try:
            # Remove duplicates based on ID
            initial_count = len(df)
            df = df.drop_duplicates(subset=['id'], keep='first')
            duplicates_removed = initial_count - len(df)
            self.quality_metrics["duplicates_removed"] = duplicates_removed
            self.log_step("TRANSFORM", f"Removed {duplicates_removed} duplicate rows")
            
            # Make a copy to avoid SettingWithCopyWarning
            df = df.copy()
            
            # Handle missing values and track them
            for column in df.columns:
                if column != '_source_file':
                    missing_count = df[column].isna().sum()
                    self.quality_metrics["missing_values"][column] = missing_count
            
            # Convert data types
            df.loc[:, 'id'] = pd.to_numeric(df['id'], errors='coerce').astype('Int64')
            df.loc[:, 'age'] = pd.to_numeric(df['age'], errors='coerce').astype('Int64')
            df.loc[:, 'salary'] = pd.to_numeric(df['salary'], errors='coerce')
            df.loc[:, 'name'] = df['name'].astype(str)
            df.loc[:, 'department'] = df['department'].astype(str)
            
            # Standardize text fields
            df.loc[:, 'name'] = df['name'].str.title().str.strip()
            df.loc[:, 'department'] = df['department'].str.upper().str.strip()
            
            # Calculate derived fields
            df.loc[:, 'yearly_salary'] = df['salary'] * 12
            
            # Remove rows with any null values after conversion
            df_clean = df.dropna()
            
            # Remove the source file column before final output
            if '_source_file' in df_clean.columns:
                df_clean = df_clean.drop('_source_file', axis=1)
            
            rows_after_cleaning = len(df_clean)
            rows_removed = original_count - rows_after_cleaning
            
            self.log_step("TRANSFORM", f"Transformation completed. {rows_removed} rows removed during cleaning")
            
            return df_clean
            
        except Exception as e:
            self.log_step("TRANSFORM", f"Error during transformation: {e}", "ERROR")
            raise
    
    def generate_quality_report(self) -> pd.DataFrame:
        """
        Generate a comprehensive data quality report.
        
        Returns:
            pd.DataFrame: Quality report as DataFrame
        """
        self.log_step("QUALITY_REPORT", "Generating data quality report")
        
        report_data = {
            "Metric": [
                "Total Rows",
                "Valid Rows",
                "Invalid Rows",
                "Duplicates Removed",
                "Schema Violations",
                "Files Processed",
                "Files Failed",
                "Success Rate (%)"
            ],
            "Value": [
                self.quality_metrics["total_rows"],
                self.quality_metrics["valid_rows"],
                self.quality_metrics["invalid_rows"],
                self.quality_metrics["duplicates_removed"],
                self.quality_metrics["schema_violations"],
                self.quality_metrics["files_processed"],
                self.quality_metrics["files_failed"],
                round((self.quality_metrics["valid_rows"] / max(self.quality_metrics["total_rows"], 1)) * 100, 2)
            ]
        }
        
        # Add missing values per column
        for column, missing_count in self.quality_metrics["missing_values"].items():
            report_data["Metric"].append(f"Missing Values - {column}")
            report_data["Value"].append(missing_count)
        
        report_df = pd.DataFrame(report_data)
        
        # Save report to CSV
        try:
            report_filename = f"quality_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            report_df.to_csv(report_filename, index=False)
            self.log_step("QUALITY_REPORT", f"Quality report saved to {report_filename}")
        except Exception as e:
            self.log_step("QUALITY_REPORT", f"Failed to save quality report: {e}", "ERROR")
        
        # Print console-friendly table
        if self.verbose:
            print("\n" + "="*50)
            print("DATA QUALITY REPORT")
            print("="*50)
            for index, row in report_df.iterrows():
                print(f"{row['Metric']:<25}: {row['Value']}")
            print("="*50 + "\n")
        
        return report_df
    
    def load(self, df: pd.DataFrame, invalid_rows: List[Dict]):
        """
        Load clean data to SQLite database and quarantine invalid rows.
        
        Args:
            df (pd.DataFrame): Clean data to load
            invalid_rows (List[Dict]): Invalid rows to quarantine
        """
        self.log_step("LOAD", "Starting data load to database")
        
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Load clean data (replace to avoid constraint conflicts)
                if not df.empty:
                    df.to_sql('clean_data', conn, if_exists='replace', index=False)
                    self.log_step("LOAD", f"Loaded {len(df)} clean rows to database")
                else:
                    self.log_step("LOAD", "No clean data to load", "WARNING")
                
                # Load invalid rows to quarantine (replace to avoid constraint conflicts)
                if invalid_rows:
                    quarantine_df = pd.DataFrame(invalid_rows)
                    quarantine_df.to_sql('quarantine', conn, if_exists='replace', index=False)
                    self.log_step("LOAD", f"Quarantined {len(invalid_rows)} invalid rows")
                else:
                    self.log_step("LOAD", "No invalid rows to quarantine")
                
                conn.commit()
                self.log_step("LOAD", "Data load completed successfully")
                
        except Exception as e:
            self.log_step("LOAD", f"Error during data load: {e}", "ERROR")
            raise
    
    def run_etl(self, file_paths: List[str]):
        """
        Execute the complete ETL pipeline.
        
        Args:
            file_paths (List[str]): List of CSV file paths to process
        """
        self.log_step("PIPELINE", "Starting LoadReady ETL Pipeline")
        start_time = datetime.now()
        
        try:
            # Extract
            dataframes = self.extract(file_paths)
            
            if not dataframes:
                self.log_step("PIPELINE", "No data extracted. Pipeline terminated.", "ERROR")
                return
            
            # Validate Schema
            valid_df, invalid_rows = self.validate_schema(dataframes)
            
            # Transform
            transformed_df = self.transform(valid_df)
            
            # Load
            self.load(transformed_df, invalid_rows)
            
            # Generate Quality Report
            self.generate_quality_report()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.log_step("PIPELINE", f"ETL Pipeline completed successfully in {duration:.2f} seconds")
            
        except Exception as e:
            self.log_step("PIPELINE", f"ETL Pipeline failed: {e}", "ERROR")
            self.log_step("PIPELINE", f"Stack trace: {traceback.format_exc()}", "ERROR")
            raise


def main():
    """
    Main function to handle command-line interface and execute ETL pipeline.
    """
    parser = argparse.ArgumentParser(
        description="LoadReady - Production-Ready Python ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python loadready.py data.csv
    python loadready.py file1.csv file2.csv --verbose
    
Sample CSV Format:
    id,name,age,salary,department
    1,John Doe,30,5000.0,Engineering
    2,Jane Smith,25,4500.0,Marketing
    3,Bob Johnson,35,6000.0,Engineering
        """
    )
    
    parser.add_argument(
        'files',
        nargs='+',
        help='CSV files to process'
    )
    
    parser.add_argument(
        '--verbose',
        action='store_true',
        help='Print logs to console'
    )
    
    parser.add_argument(
        '--db-path',
        default='loadready.db',
        help='Path to SQLite database file (default: loadready.db)'
    )
    
    parser.add_argument(
        '--log-path',
        default='loadready.log',
        help='Path to log file (default: loadready.log)'
    )
    
    args = parser.parse_args()
    
    # Validate file arguments
    if not args.files:
        print("Error: No input files specified")
        sys.exit(1)
    
    try:
        # Initialize and run ETL pipeline
        etl = LoadReadyETL(
            db_path=args.db_path,
            log_path=args.log_path,
            verbose=args.verbose
        )
        
        etl.run_etl(args.files)
        
        print(f"\nETL Pipeline completed successfully!")
        print(f"Database: {args.db_path}")
        print(f"Log file: {args.log_path}")
        
        if not args.verbose:
            print("Use --verbose flag to see detailed logs in console")
            
    except KeyboardInterrupt:
        print("\nETL Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nETL Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()


"""
Sample CSV Data for Testing:
============================

Create test files with the following content:

File: sample1.csv
id,name,age,salary,department
1,john doe,30,5000.0,engineering
2,jane smith,25,4500.0,marketing
3,bob johnson,35,6000.0,engineering
4,alice brown,28,5200.0,hr

File: sample2.csv
id,name,age,salary,department
5,charlie wilson,32,5500.0,engineering
6,diana prince,29,4800.0,marketing
7,edward norton,45,7000.0,management
1,john doe,30,5000.0,engineering

File: invalid_sample.csv
id,name,age,salary,department
8,frank miller,invalid_age,5300.0,sales
9,,27,4900.0,engineering
10,grace kelly,31,,hr
11,henry ford,34,5600.0,

Usage Examples:
===============
python loadready.py sample1.csv sample2.csv --verbose
python loadready.py *.csv
python loadready.py sample1.csv --db-path custom.db --log-path custom.log

The pipeline will:
1. Extract data from all specified CSV files
2. Validate against the expected schema
3. Remove duplicates and clean data
4. Generate a quality report
5. Load clean data to SQLite database
6. Quarantine invalid rows with error details
7. Provide comprehensive logging
"""
