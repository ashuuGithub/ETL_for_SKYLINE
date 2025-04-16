import pandas as pd
import pymysql
import pyodbc
import sshtunnel
import logging
import urllib.parse
from sqlalchemy import create_engine

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
        logger.info(f"Executing query: {query}")
        df = pd.read_sql(query, sql_conn)
        logger.info(f"Fetched {len(df)} rows from {table_name}")
        return df
    except Exception as e:
        logger.error(f"Failed to fetch data from {table_name}: {str(e)}")
        raise

def load_data_to_mysql(mysql_conn, mysql_config, table_name, df, mysql_columns, batch_size=10000, truncate=False):
    """Load data into MySQL table in batches with optional truncate"""
    try:
        cursor = mysql_conn.cursor()
        
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
            f"mysql+pymysql://{mysql_config['user']}:{encoded_password}@{mysql_config['host']}/{mysql_config['database']}"
        )
        
        # Load data in batches
        total_rows = len(df)
        for start in range(0, total_rows, batch_size):
            batch_df = df.iloc[start:start + batch_size]
            batch_df.to_sql(table_name, mysql_engine, if_exists='append', index=False)
            logger.info(f"Inserted batch {start//batch_size + 1}: {len(batch_df)} rows")
        
        # Verify final count
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        after_count = cursor.fetchone()[0]
        logger.info(f"Total records in target table after insertion: {after_count}")
        
    except Exception as e:
        logger.error(f"Failed to load data to {table_name}: {str(e)}")
        mysql_conn.rollback()
        raise
    finally:
        cursor.close()

def main():
    # Configuration
    ssh_config = {
        'host': '54.177.119.221',
        'username': 'ec2-user',
        'password': None,
        'private_key_path': r'C:\Users\AshishKumarSen\Downloads\EC2_Skyline_Key.pem',
        'remote_host': 'skylineaz.infinitecampus.org',
        'remote_port': 7771
    }
    sql_server_config = {
        'database': 'skyline',
        'user': 'SkylineEducation_ArshadHayat',
        'password': 'kukaPUBReJlCoF4lZina'
    }
    mysql_config = {
        'host': 'b2b-s360.chpxcjdw4aj9.ap-south-1.rds.amazonaws.com',
        'user': 'B2B_Admin',
        'password': 'b2b@123',
        'database': 'skyline_bkp'
    }
    
    # Table and column mappings
    table_name = 'Calendar'
    # SQL Server columns (source)
    sql_columns = [
        'calendarID',
        'districtID',
        'schoolID',
        'endYear',
        'name',
        'number',
        'startDate',
        'endDate',
        'comments',
        'exclude',
        'summerSchool',
        'studentDay',
        'teacherDay',
        'wholeDayAbsence',
        'halfDayAbsence',
        'calendarGUID',
        'alternativeCode',
        'title3',
        'title3consortium',
        'title1',
        'legacyKey',
        'schoolChoice',
        'type',
        'countDate',
        'assignmentRequired',
        'sifExclude',
        'positiveAttendanceEnabled',
        'positiveAttendanceEditDays',
        'track',
        'typeBIE',
        'sequence',
        'externalLMSExclude',
        'attendanceType',
        'echs',
        'stem',
        'programType',
        'deleteIndicator',
        'deleteOrigin',
        'deleteReasonCode',
        'deleteReasonComments',
        'deleteRequestedByID',
        'deleteRequestedTimestamp',
        'deleteStatus',
        'deleteFailureReason',
        'deleteRequestedByGUID',
        'foodServiceEnrollOverride',
        'secondarySchool',
        'virtual',
        'ignoreCourseMasterPush',
        'rolledForwardID',
        'crossSiteEnrollmentOpen'
    ]
    # MySQL columns (target)
    mysql_columns = [
        'calendarID',
        'districtID',
        'schoolID',
        'endYear',
        'name',
        'number',
        'startDate',
        'endDate',
        'comments',
        'exclude',
        'summerSchool',
        'studentDay',
        'teacherDay',
        'wholeDayAbsence',
        'halfDayAbsence',
        'calendarGUID',
        'alternativeCode',
        'title3',
        'title3consortium',
        'title1',
        'legacyKey',
        'schoolChoice',
        'type',
        'countDate',
        'assignmentRequired',
        'sifExclude',
        'positiveAttendanceEnabled',
        'positiveAttendanceEditDays',
        'track',
        'typeBIE',
        'sequence',
        'externalLMSExclude',
        'attendanceType',
        'echs',
        'stem',
        'programType',
        'deleteIndicator',
        'deleteOrigin',
        'deleteReasonCode',
        'deleteReasonComments',
        'deleteRequestedByID',
        'deleteRequestedTimestamp',
        'deleteStatus',
        'deleteFailureReason',
        'deleteRequestedByGUID',
        'foodServiceEnrollOverride',
        'secondarySchool',
        'virtual',
        'ignoreCourseMasterPush',
        'rolledForwardID',
        'crossSiteEnrollmentOpen'
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