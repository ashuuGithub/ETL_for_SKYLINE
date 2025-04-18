import pandas as pd
import pymysql
import pyodbc
import sshtunnel
import logging
import urllib.parse
import time
from sqlalchemy import create_engine
import warnings
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

warnings.filterwarnings("ignore", category=UserWarning)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_ssh_tunnel(ssh_host, ssh_username, ssh_password, ssh_private_key_path, remote_host, remote_port):
    """Create SSH tunnel to SQL Server"""
    try:
        tunnel = sshtunnel.SSHTunnelForwarder(
            ssh_host,
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            ssh_private_key=ssh_private_key_path,
            remote_bind_address=(remote_host, remote_port),
            local_bind_address=('127.0.0.1', 0),
        )
        tunnel.start()
        logger.info(f"SSH tunnel established on local port {tunnel.local_bind_port}")
        return tunnel
    except Exception as e:
        logger.error(f"Failed to create SSH tunnel: {str(e)}")
        raise

def get_sql_server_connection(tunnel, sql_server_db, sql_server_user, sql_server_password):
    """Create connection to SQL Server through SSH tunnel"""
    try:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER=127.0.0.1,{tunnel.local_bind_port};"
            f"DATABASE={sql_server_db};"
            f"UID={sql_server_user};"
            f"PWD={sql_server_password}"
        )
        conn = pyodbc.connect(conn_str)
        logger.info("Connected to SQL Server")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to SQL Server: {str(e)}")
        raise

def get_mysql_connection(mysql_host, mysql_user, mysql_password, mysql_db):
    """Create connection to MySQL"""
    try:
        conn = pymysql.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_db
        )
        logger.info("Connected to MySQL")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to MySQL: {str(e)}")
        raise

def fetch_data(sql_conn, table_name, sql_columns):
    """Fetch specified columns from SQL Server table"""
    try:
        column_str = ', '.join([f'[{col}]' for col in sql_columns])
        query = f"SELECT {column_str} FROM dbo.{table_name}"
        df = pd.read_sql(query, sql_conn)
        
        # Check for duplicate enrollmentGUID
        if 'enrollmentGUID' in df.columns:
            duplicates = df[df['enrollmentGUID'].duplicated(keep=False)]
            if not duplicates.empty:
                logger.error(f"Duplicate enrollmentGUID values found: {duplicates['enrollmentGUID']}")
                raise ValueError("Duplicate enrollmentGUID values")
        
        logger.info(f"Fetched {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch data from {table_name}: {str(e)}")
        raise

def load_data_to_mysql(mysql_conn, mysql_config, table_name, df, mysql_columns, batch_size=10000, truncate=False):
    """Load data into MySQL table in batches with optional truncate and retry logic"""
    try:
        cursor = mysql_conn.cursor()
        
        # Disable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
        mysql_conn.commit()
        logger.info("Foreign key checks disabled")
        
        # Get initial count
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        before_count = cursor.fetchone()[0]
        logger.info(f"Records in target table '{table_name}' before operation: {before_count}")
        
        # Truncate table if requested
        if truncate:
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
            mysql_conn.commit()
            logger.info(f"Successfully truncated table '{table_name}'")
        
        # Rename DataFrame columns to match MySQL target columns
        df.columns = mysql_columns
        
        # Create SQLAlchemy engine
        encoded_password = urllib.parse.quote(mysql_config['password'])
        mysql_engine = create_engine(
            f"mysql+pymysql://{mysql_config['user']}:{encoded_password}@{mysql_config['host']}/{mysql_config['database']}?charset=utf8mb4"
        )
        
        # Load data in batches
        total_rows = len(df)
        total_inserted = 0
        for start in range(0, total_rows, batch_size):
            batch_df = df.iloc[start:start + batch_size]
            try:
                batch_df.to_sql(table_name, mysql_engine, if_exists='append', index=False)
                total_inserted += len(batch_df)
                logger.info(f"Inserted batch {start//batch_size + 1}: {len(batch_df)} rows into '{table_name}'")
            except Exception as e:
                logger.error(f"Error in batch {start//batch_size + 1} for '{table_name}': {str(e)}")
                mysql_conn.rollback()
                time.sleep(5)
                try:
                    batch_df.to_sql(table_name, mysql_engine, if_exists='append', index=False)
                    total_inserted += len(batch_df)
                    logger.info(f"Successfully retried batch {start//batch_size + 1}: {len(batch_df)} rows into '{table_name}'")
                except Exception as retry_e:
                    logger.error(f"Retry failed for batch {start//batch_size + 1} in '{table_name}': {str(retry_e)}")
                    mysql_conn.rollback()
                    continue
        
        # Verify final count
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        after_count = cursor.fetchone()[0]
        logger.info(f"Attempted to insert {total_rows} rows into '{table_name}'")
        logger.info(f"Successfully inserted {total_inserted} rows into '{table_name}'")
        logger.info(f"Total records in target table '{table_name}' after insertion: {after_count}")
        
        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")
        mysql_conn.commit()
        logger.info("Foreign key checks re-enabled")
        
    except Exception as e:
        logger.error(f"Failed to load data to '{table_name}': {str(e)}")
        mysql_conn.rollback()
        raise
    finally:
        cursor.close()

def main():
    # Configuration from .env
    ssh_config = {
        'host': os.getenv('SSH_HOST'),
        'username': os.getenv('SSH_USERNAME'),
        'password': os.getenv('SSH_PASSWORD') or None,  # Handle empty password
        'private_key_path': os.getenv('SSH_PRIVATE_KEY_PATH'),
        'remote_host': os.getenv('SSH_REMOTE_HOST'),
        'remote_port': int(os.getenv('SSH_REMOTE_PORT'))
    }
    sql_server_config = {
        'database': os.getenv('SQL_SERVER_DATABASE'),
        'user': os.getenv('SQL_SERVER_USER'),
        'password': os.getenv('SQL_SERVER_PASSWORD')
    }
    mysql_config = {
        'host': os.getenv('MYSQL_HOST'),
        'user': os.getenv('MYSQL_USER'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'database': os.getenv('MYSQL_DATABASE')
    }
    
    # Table and column mappings
    table_name = 'Enrollment'
    sql_columns = [
        'enrollmentID','personID','calendarID','structureID','grade','serviceType', 'active', 'classRankExclude','noShow','startDate',
        'startStatus','startComments', 'endDate','endStatus', 'endComments', 'endAction','nextCalendar', 'nextGrade','diplomaDate','diplomaType','diplomaPeriod',
        'postGradPlans', 'postGradLocation','gradYear', 'stateExclude',  'servingDistrict','residentDistrict','residentSchool','mealStatus','englishProficiency','englishProficiencyDate',
        'lep','esl','language','citizenship','title1', 'title3', 'transportation','migrant','immigrant','homeless','homeSchooled','homebound','giftedTalented','nclbChoice', 
        'percentEnrolled','admOverride', 'adaOverride', 'vocationalCode', 'pseo', 'facilityCode','stateAid','stateFundingCode','section504','specialEdStatus','specialEdSetting','disability1',
        'disability2','disability3','disability4','disability5','enrollmentGUID', 'privateSchooled', 'spedExitDate','spedExitReason','childCountStatus','grade9Date','singleParent',
        'displacedHomemaker','legacyKey','legacySeq1', 'legacySeq2','endYear','districtID','localStudentNumber','modifiedDate','modifiedByID','dropoutCode','eip', 'adult','servingCounty',
        'attendanceGroup','projectedGraduationDate','withdrawDate','rollForwardCode','rollForwardEnrollmentID','cohortYear','disability6','disability7','disability8','disability9',
        'disability10','nextStructureID','schoolEntryDate','districtEntryDate','mvUnaccompaniedYouth','externalLMSExclude','schoolOfAccountability','localStartStatusTypeID',
        'localEndStatusTypeID','schoolChoiceProgram','dpsaCalculatedTier', 'dpsaReportedTier', 'excludeFromDpsaCalculation','crossSiteEnrollment','peerID','choiceBasisReason'
    ]
    mysql_columns = [
        'enrollmentID','personID','calendarID','structureID','grade','serviceType', 'active', 'classRankExclude','noShow','startDate',
        'startStatus','startComments', 'endDate','endStatus', 'endComments', 'endAction','nextCalendar', 'nextGrade','diplomaDate','diplomaType','diplomaPeriod',
        'postGradPlans', 'postGradLocation','gradYear', 'stateExclude',  'servingDistrict','residentDistrict','residentSchool','mealStatus','englishProficiency','englishProficiencyDate',
        'lep','esl','language','citizenship','title1', 'title3', 'transportation','migrant','immigrant','homeless','homeSchooled','homebound','giftedTalented','nclbChoice', 
        'percentEnrolled','admOverride', 'adaOverride', 'vocationalCode', 'pseo', 'facilityCode','stateAid','stateFundingCode','section504','specialEdStatus','specialEdSetting','disability1',
        'disability2','disability3','disability4','disability5','enrollmentGUID', 'privateSchooled', 'spedExitDate','spedExitReason','childCountStatus','grade9Date','singleParent',
        'displacedHomemaker','legacyKey','legacySeq1', 'legacySeq2','endYear','districtID','localStudentNumber','modifiedDate','modifiedByID','dropoutCode','eip', 'adult','servingCounty',
        'attendanceGroup','projectedGraduationDate','withdrawDate','rollForwardCode','rollForwardEnrollmentID','cohortYear','disability6','disability7','disability8','disability9',
        'disability10','nextStructureID','schoolEntryDate','districtEntryDate','mvUnaccompaniedYouth','externalLMSExclude','schoolOfAccountability','localStartStatusTypeID',
        'localEndStatusTypeID','schoolChoiceProgram','dpsaCalculatedTier', 'dpsaReportedTier', 'excludeFromDpsaCalculation','crossSiteEnrollment','peerID','choiceBasisReason'
    ]
    
    try:
        # Create SSH tunnel
        tunnel = create_ssh_tunnel(
            ssh_config['host'],
            ssh_config['username'],
            ssh_config['password'],
            ssh_config['private_key_path'],
            ssh_config['remote_host'],
            ssh_config['remote_port']
        )
        
        # Connect to databases
        sql_conn = get_sql_server_connection(
            tunnel,
            sql_server_config['database'],
            sql_server_config['user'],
            sql_server_config['password']
        )
        mysql_conn = get_mysql_connection(
            mysql_config['host'],
            mysql_config['user'],
            mysql_config['password'],
            mysql_config['database']
        )
        
        # Fetch and load data
        df = fetch_data(sql_conn, table_name, sql_columns)
        if not df.empty:
            load_data_to_mysql(mysql_conn, mysql_config, table_name, df, mysql_columns,
                             batch_size=10000, truncate=True)
        else:
            logger.warning("No data retrieved from source table")
            
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        raise
    finally:
        # Cleanup
        if 'sql_conn' in locals():
            sql_conn.close()
            logger.info("SQL Server connection closed")
        if 'mysql_conn' in locals():
            mysql_conn.close()
            logger.info("MySQL connection closed")
        if 'tunnel' in locals():
            tunnel.stop()
            logger.info("SSH tunnel closed")

if __name__ == "__main__":
    main()