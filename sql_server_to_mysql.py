import pandas as pd
import pymysql
import pyodbc
import sshtunnel
from sqlalchemy import create_engine
import logging
import urllib.parse

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
            local_bind_address=('127.0.0.1', 0),  # Auto-assign local port
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

def get_table_list(sql_conn):
    """Get list of specified tables and views from SQL Server skyline.dbo schema"""
    try:
        cursor = sql_conn.cursor()
        desired_objects = ['person', 'identity', 'Enrollment', 'calender', 'pronoun', 
                          'student', 'staffmember', 'individual']
        
        # Get both tables and views from dbo schema
        query = """
        SELECT TABLE_NAME 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = 'dbo' 
        AND TABLE_NAME IN (%s)
        AND (TABLE_TYPE = 'BASE TABLE' OR TABLE_TYPE = 'VIEW')
        """ % ','.join(['?'] * len(desired_objects))
        
        cursor.execute(query, desired_objects)
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(tables)} matching tables/views in SQL Server: {tables}")
        return tables
    except Exception as e:
        logger.error(f"Failed to get table list: {str(e)}")
        raise

def create_table_in_mysql(mysql_conn, table_name, columns_info):
    """Create table in MySQL based on SQL Server schema"""
    try:
        cursor = mysql_conn.cursor()
        # Expanded SQL Server to MySQL type mapping
        type_mapping = {
            'int': 'INT',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'TINYINT',
            'varchar': 'VARCHAR',
            'nvarchar': 'VARCHAR',
            'char': 'CHAR',
            'nchar': 'CHAR',
            'text': 'TEXT',
            'ntext': 'TEXT',
            'datetime': 'DATETIME',
            'datetime2': 'DATETIME',
            'date': 'DATE',
            'time': 'TIME',
            'float': 'FLOAT',
            'decimal': 'DECIMAL',
            'numeric': 'DECIMAL',
            'bit': 'BOOLEAN',
            'uniqueidentifier': 'CHAR(36)',
            'binary': 'BINARY',
            'varbinary': 'VARBINARY'
        }
        columns = []
        # Use TEXT for VARCHAR in large tables to avoid row size limit
        use_text_for_varchar = len(columns_info) > 50  # Threshold for large tables
        estimated_row_size = 0
        for col in columns_info:
            col_name = col[0]
            col_type = col[1].lower()
            # Extract base type (before parentheses)
            base_type = col_type.split('(')[0]
            mysql_type = type_mapping.get(base_type, 'TEXT')
            # Handle special cases
            if col_type.startswith(('varchar(max)', 'nvarchar(max)')):
                mysql_type = 'TEXT'
                estimated_row_size += 12  # Pointer size for TEXT
            elif base_type in ('varchar', 'nvarchar'):
                if use_text_for_varchar:
                    mysql_type = 'TEXT'
                    estimated_row_size += 12
                else:
                    # Check if length is specified
                    if '(' not in col_type:
                        mysql_type = 'VARCHAR(255)'
                        estimated_row_size += 255 * 3  # utf8mb4
                    else:
                        # Preserve specified length
                        length = col_type[col_type.find('(')+1:col_type.find(')')]
                        if length.isdigit():
                            mysql_type = f"VARCHAR({length})"
                            estimated_row_size += int(length) * 3
                        else:
                            mysql_type = 'TEXT'
                            estimated_row_size += 12
            elif base_type in ('char', 'nchar') and '(' not in col_type:
                mysql_type = 'CHAR(1)'
                estimated_row_size += 3
            elif base_type in ('decimal', 'numeric') and '(' not in col_type:
                mysql_type = 'DECIMAL(10,2)'
                estimated_row_size += 9  # Approx
            elif '(' in col_type and mysql_type != 'TEXT':
                # Preserve precision for other types
                mysql_type += f"{col_type[col_type.find('('):col_type.find(')')+1]}"
                if base_type in ('decimal', 'numeric'):
                    estimated_row_size += 9
                elif base_type in ('char', 'nchar'):
                    length = col_type[col_type.find('(')+1:col_type.find(')')]
                    estimated_row_size += int(length) * 3 if length.isdigit() else 3
            elif mysql_type == 'INT':
                estimated_row_size += 4
            elif mysql_type == 'BIGINT':
                estimated_row_size += 8
            elif mysql_type == 'SMALLINT':
                estimated_row_size += 2
            elif mysql_type == 'TINYINT':
                estimated_row_size += 1
            elif mysql_type == 'DATETIME':
                estimated_row_size += 8
            elif mysql_type == 'DATE':
                estimated_row_size += 3
            elif mysql_type == 'FLOAT':
                estimated_row_size += 4
            elif mysql_type == 'BOOLEAN':
                estimated_row_size += 1
            elif mysql_type == 'CHAR(36)':
                estimated_row_size += 36 * 3
            else:
                estimated_row_size += 12  # Default for TEXT/BLOB
            columns.append(f"`{col_name}` {mysql_type}")
        logger.info(f"Estimated row size for {table_name}: {estimated_row_size} bytes")
        create_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns)})"
        logger.info(f"Generated CREATE TABLE query: {create_query}")
        cursor.execute(create_query)
        mysql_conn.commit()
        logger.info(f"Created table {table_name} in MySQL")
    except Exception as e:
        logger.error(f"Failed to create table {table_name} in MySQL: {str(e)}")
        raise

def transfer_data(sql_conn, mysql_conn, mysql_config, table_name):
    """Transfer data from SQL Server to MySQL for a single table"""
    try:
        # Get column information
        cursor = sql_conn.cursor()
        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}' AND TABLE_SCHEMA = 'dbo'")
        columns_info = cursor.fetchall()
        # Create table in MySQL
        create_table_in_mysql(mysql_conn, table_name, columns_info)
        # Read data from SQL Server with chunking
        query = f"SELECT * FROM dbo.{table_name}"
        chunk_size = 10000  # Adjust based on memory constraints
        # Create SQLAlchemy engine for MySQL with URL-encoded password
        encoded_password = urllib.parse.quote(mysql_config['password'])
        mysql_engine = create_engine(
            f"mysql+pymysql://{mysql_config['user']}:{encoded_password}@{mysql_config['host']}/{mysql_config['database']}"
        )
        # Transfer data in chunks
        for chunk in pd.read_sql(query, sql_conn, chunksize=chunk_size):
            chunk.to_sql(table_name, mysql_engine, if_exists='append', index=False)
            logger.info(f"Transferred {len(chunk)} rows to {table_name}")
    except Exception as e:
        logger.error(f"Failed to transfer data for table {table_name}: {str(e)}")
        raise

def main():
    # Configuration (hardcoded)
    ssh_config = {
        'host': '54.177.119.221',
        'username': 'ec2-user',
        'password': None,  # Set to None for key-based auth
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
        'database': 'skyline'
    }
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
        # Get specified tables and views
        tables = get_table_list(sql_conn)
        # Transfer each table/view
        for table in tables:
            logger.info(f"Processing table/view: {table}")
            try:
                transfer_data(sql_conn, mysql_conn, mysql_config, table)
            except Exception as e:
                logger.error(f"Skipping table/view {table} due to error: {str(e)}")
                continue
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


