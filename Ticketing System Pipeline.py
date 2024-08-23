import logging
import mysql.connector
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set the logging level
    format='%(asctime)s - %(levelname)s - %(message)s',  # Set the log message format
    handlers=[
        logging.FileHandler("event_ticket_pipeline.log"),  # Log to a file
        logging.StreamHandler()  # Also output logs to console
    ]
)

def get_db_connection():
    """
    Establishes a connection to the MySQL database.

    Returns:
        connection (mysql.connector.connection.MySQLConnection): MySQL database connection object.
    """
    connection = None
    try:
        logging.info("Attempting to connect to the database.")
        connection = mysql.connector.connect(
            user='march',
            password='root',
            host='127.0.0.1',
            port='3306',
            database='event_ticket_system'
        )
        logging.info("Successfully connected to the database.")
    except mysql.connector.Error as error:
        logging.error(f"Error while connecting to the database: {error}")
    return connection

def load_third_party(connection, file_path_csv):
    """
    Loads third-party ticket sales data from a CSV file into the 'sales' table.

    Args:
        connection (mysql.connector.connection.MySQLConnection): MySQL database connection object.
        file_path_csv (str): Path to the CSV file containing the sales data.
    """
    cursor = connection.cursor()
    try:
        logging.info(f"Loading data from {file_path_csv} into the sales table.")
        with open(file_path_csv, mode='r') as file:
            csv_reader = csv.reader(file)
            for row in csv_reader:
                insert_statement = """
                INSERT INTO sales (ticket_id, trans_date, event_id, event_name, event_date, event_type, event_city, customer_id, price, num_tickets)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(insert_statement, row)
        connection.commit()
        logging.info("Data loaded successfully into the sales table.")
    except Exception as error:
        logging.error(f"Error while loading data into the sales table: {error}")
    finally:
        cursor.close()
        logging.info("Cursor closed.")

def query_popular_tickets(connection):
    """
    Queries the 'sales' table to find the most popular events in the past month.

    Args:
        connection (mysql.connector.connection.MySQLConnection): MySQL database connection object.

    Returns:
        list of tuples: The most popular events and their total ticket sales.
    """
    sql_statement = """
    SELECT event_name, SUM(num_tickets) as total_tickets_sold
    FROM sales
    WHERE trans_date >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
    GROUP BY event_name
    ORDER BY total_tickets_sold DESC
    LIMIT 3;
    """
    cursor = connection.cursor()
    records = []
    try:
        logging.info("Executing query to find the most popular events in the past month.")
        cursor.execute(sql_statement)
        records = cursor.fetchall()
        logging.info("Query executed successfully.")
    except Exception as error:
        logging.error(f"Error while querying the sales table: {error}")
    finally:
        cursor.close()
        logging.info("Cursor closed.")
    return records

def display_popular_tickets(records):
    """
    Displays the most popular events in a user-friendly format.

    Args:
        records (list of tuples): The most popular events and their total ticket sales.
    """
    logging.info("Displaying the most popular tickets in the past month.")
    print("Here are the most popular tickets in the past month:")
    for record in records:
        print(f"- {record[0]}")
    logging.info("Displayed the popular tickets successfully.")

if __name__ == "__main__":
    logging.info("Starting the Event Ticket System Data Pipeline.")

    # Establish the database connection
    connection = get_db_connection()
    if connection:
        # Load data from CSV to MySQL
        load_third_party(connection, 'third_party_sales_1.csv')

        # Query and display popular tickets
        records = query_popular_tickets(connection)
        display_popular_tickets(records)

        # Close the database connection
        connection.close()
        logging.info("Database connection closed.")
    else:
        logging.error("Failed to connect to the database.")
