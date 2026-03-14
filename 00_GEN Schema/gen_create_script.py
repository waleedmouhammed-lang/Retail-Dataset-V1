import os
import re
import sys
import time
import hashlib
import sqlite3
import logging
import pyodbc
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from typing import Dict, List, Set

# ==============================================================================
# CONFIGURATION
# ==============================================================================
class Config:
    # Database connection parameters (Secured via Environment Variables)
    SERVER = os.environ.get('DB_SERVER', 'localhost')
    DATABASE = os.environ.get('DB_NAME', 'ContosoRetailDW')
    DRIVER = os.environ.get('DB_DRIVER', '{ODBC Driver 17 for SQL Server}')
    
    # Secure credential extraction
    UID = os.environ.get('DB_UID')
    PWD = os.environ.get('DB_PWD')
    
    # Dynamically build the connection string based on provided credentials
    if UID and PWD:
        # Production: SQL Server Authentication using injected secrets
        CONN_STR = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={UID};PWD={PWD};"
    else:
        # Local/Dev: Fallback to Windows Authentication
        CONN_STR = f"DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    
    # State tracking database (Local SQLite)
    STATE_DB = 'pipeline_state.db'
    
    # Logs
    LOG_FILE = 'pipeline_execution.log'
    
    # Pipeline Settings
    MAX_WORKERS = 4 # Maximum concurrent SQL scripts
    
    # The DAG (Directed Acyclic Graph)
    # Key: Script filename
    # Value: List of scripts that MUST complete successfully before this key runs
    DAG_GRAPH = {
        '00_create_schemas_v2.sql': [],
        '01_gen_ReferenceDimensions_v2.sql': ['00_create_schemas_v2.sql'],
        '02_gen_CustomerAcquisition_v2.sql': ['01_gen_ReferenceDimensions_v2.sql'],
        '03_gen_OrderPayment_v2.sql': ['01_gen_ReferenceDimensions_v2.sql'],
        '04_gen_OrderFulfillment_v2.sql': ['00_create_schemas_v2.sql'],
        '05_gen_FactMarketingSpend_v2.sql': ['02_gen_CustomerAcquisition_v2.sql'],
        '06_gen_FactCustomerSurvey_v2.sql': ['01_gen_ReferenceDimensions_v2.sql'],
        '07_gen_OnlineReturnEvents_v2.sql': ['01_gen_ReferenceDimensions_v2.sql'],
        '08_gen_PhysicalReturnEvents_v2.sql': ['01_gen_ReferenceDimensions_v2.sql']
    }

# ==============================================================================
# SETUP LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s',
    handlers=[
        logging.FileHandler(Config.LOG_FILE, mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ==============================================================================
# STATE MANAGEMENT
# ==============================================================================
class StateManager:
    """Manages pipeline state using a local SQLite database for idempotency."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS script_state (
                    script_name TEXT PRIMARY KEY,
                    file_hash TEXT,
                    status TEXT,
                    last_run_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    duration_seconds REAL,
                    error_message TEXT
                )
            ''')

    def get_successful_scripts(self) -> Set[str]:
        """Returns a set of scripts that have successfully executed with their current hash."""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT script_name, file_hash FROM script_state WHERE status = 'SUCCESS'")
            return {row[0]: row[1] for row infetchall()}

    def mark_started(self, script_name: str, file_hash: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT INTO script_state (script_name, file_hash, status, error_message)
                VALUES (?, ?, 'RUNNING', NULL)
                ON CONFLICT(script_name) DO UPDATE SET 
                    file_hash=excluded.file_hash, 
                    status='RUNNING', 
                    last_run_timestamp=CURRENT_TIMESTAMP,
                    error_message=NULL
            ''', (script_name, file_hash))

    def mark_completed(self, script_name: str, duration: float):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE script_state 
                SET status = 'SUCCESS', duration_seconds = ? 
                WHERE script_name = ?
            ''', (duration, script_name))

    def mark_failed(self, script_name: str, error_msg: str):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                UPDATE script_state 
                SET status = 'FAILED', error_message = ? 
                WHERE script_name = ?
            ''', (error_msg, script_name))

# ==============================================================================
# SQL EXECUTION ENGINE
# ==============================================================================
def get_file_hash(filepath: str) -> str:
    """Generates an MD5 hash of the file to detect if it has changed since last run."""
    hasher = hashlib.md5()
    with open(filepath, 'rb') as f:
        buf = f.read()
        hasher.update(buf)
    return hasher.hexdigest()

def execute_sql_file(filepath: str, conn_str: str):
    """Parses a SQL file by 'GO' statements and executes batches sequentially."""
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Script not found: {filepath}")

    with open(filepath, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # Regex to split T-SQL batches by 'GO' (case-insensitive, on its own line)
    # This prevents pyodbc syntax errors
    batches = re.split(r'(?i)^\s*GO\s*$', sql_content, flags=re.MULTILINE)

    # Autocommit must be True for certain DDL commands (like creating databases/schemas)
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        cursor = conn.cursor()
        
        for idx, batch in enumerate(batches):
            batch = batch.strip()
            if not batch:
                continue
            
            try:
                cursor.execute(batch)
                
                # Fetch results if the batch was a SELECT statement (e.g., your DQ checks)
                if cursor.description:
                    rows = cursor.fetchall()
                    logger.info(f"[{os.path.basename(filepath)}] Batch {idx+1} Output:")
                    for row in rows:
                        logger.info(f"  -> {row}")
                        
                # Handle multiple result sets if present
                while cursor.nextset():
                    if cursor.description:
                        rows = cursor.fetchall()
                        for row in rows:
                            logger.info(f"  -> {row}")

            except pyodbc.Error as e:
                # Capture the exact line and error from SQL Server
                error_sql_state = e.args[0]
                error_msg = e.args[1]
                logger.error(f"SQL Error in {os.path.basename(filepath)} at batch {idx+1}:\n{error_msg}")
                raise RuntimeError(f"Batch {idx+1} failed: {error_msg}") from e

# ==============================================================================
# PIPELINE ORCHESTRATOR
# ==============================================================================
def run_pipeline():
    logger.info("Starting Z.Analytics Production Pipeline...")
    state_manager = StateManager(Config.STATE_DB)
    successful_state = state_manager.get_successful_scripts()
    
    # Validation: Ensure all files in DAG actually exist
    for script in Config.DAG_GRAPH.keys():
        if not os.path.exists(script):
            logger.error(f"CRITICAL: Defined script '{script}' is missing from the directory.")
            sys.exit(1)

    completed_nodes = set()
    failed_nodes = set()
    futures_map = {} # Mapping Future -> script_name

    with ThreadPoolExecutor(max_workers=Config.MAX_WORKERS) as executor:
        
        # Main DAG execution loop
        while len(completed_nodes) + len(failed_nodes) < len(Config.DAG_GRAPH):
            
            # 1. Identify ready tasks
            for script, dependencies in Config.DAG_GRAPH.items():
                if script in completed_nodes or script in failed_nodes or script in futures_map.values():
                    continue
                
                # A script is ready if ALL its dependencies are in completed_nodes
                if all(dep in completed_nodes for dep in dependencies):
                    
                    file_hash = get_file_hash(script)
                    
                    # Idempotency Check: Skip if already succeeded with the SAME hash
                    if script in successful_state and successful_state[script] == file_hash:
                        logger.info(f"Skipping {script} - Already executed successfully with current hash.")
                        completed_nodes.add(script)
                        continue
                    
                    # Submit task
                    logger.info(f"Queueing {script} for execution...")
                    state_manager.mark_started(script, file_hash)
                    
                    future = executor.submit(execute_sql_file, script, Config.CONN_STR)
                    futures_map[future] = (script, time.time()) # Store start time

            # 2. Wait for at least one running task to finish
            if not futures_map:
                if len(completed_nodes) + len(failed_nodes) < len(Config.DAG_GRAPH):
                    logger.error("PIPELINE DEADLOCK: Unable to resolve dependencies. Check DAG configuration.")
                    break

            done, _ = wait(futures_map.keys(), return_when=FIRST_COMPLETED)
            
            # 3. Process completed tasks
            for future in done:
                script_name, start_time = futures_map.pop(future)
                duration = time.time() - start_time
                
                try:
                    future.result() # This will raise any exception caught in execute_sql_file
                    logger.info(f"SUCCESS: {script_name} completed in {duration:.2f}s.")
                    state_manager.mark_completed(script_name, duration)
                    completed_nodes.add(script_name)
                    
                except Exception as e:
                    logger.error(f"FAILED: {script_name} terminated with error.")
                    state_manager.mark_failed(script_name, str(e))
                    failed_nodes.add(script_name)
                    
            # 4. Fail-fast mechanism
            if failed_nodes:
                logger.error("Pipeline halt triggered due to node failure(s). Canceling remaining execution.")
                # We break the loop. Remaining tasks will not be queued. 
                # Currently running threads will finish naturally.
                break
                
    # ==========================================================================
    # PIPELINE SUMMARY
    # ==========================================================================
    logger.info("========================================")
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("========================================")
    logger.info(f"Total Nodes  : {len(Config.DAG_GRAPH)}")
    logger.info(f"Completed    : {len(completed_nodes)}")
    logger.info(f"Failed       : {len(failed_nodes)}")
    logger.info(f"Pending/Skip : {len(Config.DAG_GRAPH) - len(completed_nodes) - len(failed_nodes)}")
    
    if failed_nodes:
        logger.error(f"Failed Scripts: {', '.join(failed_nodes)}")
        sys.exit(1)
    else:
        logger.info("Pipeline completed successfully. All data quality checks passed execution.")
        sys.exit(0)

if __name__ == '__main__':
    run_pipeline()