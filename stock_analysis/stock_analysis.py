import yfinance as yf
import pandas as pd
import pyodbc

import stock_analysis.consts as c

# Create a connection string

def get_database_credential_connections(server_conn, server_name, database_name):
    conn_str = (
        r'DRIVER=server_conn;'
        r'SERVER=server_name;'
        r'DATABASE=database_name;'
        r'Trusted_Connection=yes;'
    )

    return conn_str

def connect_to_database(server_conn, server_name, database_name):
    # Connect to the SQL Server database
    conn_str = get_database_credential_connections(server_conn, server_name, database_name)
    conn = pyodbc.connect(conn_str)
    return conn


def get_stock_data():

    # Iterate over each ticker symbol
    for ticker in c.tickers:
        # Step 1: Download historical data for the ticker symbol
        data = yf.download(ticker, start='2022-01-01', end='2023-12-31')

        # Step 2: Add a new column for the ticker symbol
        data['Symbol'] = ticker

    return data

def commit_data_to_database(data, server_conn, server_name, database_name):

    conn = connect_to_database(server_conn, server_name, database_name)

    try:
        # Step 3: Write the data from the DataFrame to the SQL Server table
        for index, row in data.iterrows():
            cursor = conn.cursor()
            cursor.execute("INSERT INTO YourTableName VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                           row.name, row['Open'], row['High'], row['Low'], row['Close'], row['Adj Close'], row['Volume'], row['Symbol'])
            conn.commit()

        cursor.close()
        conn.close()

        return True

    except:
        Exception("Failure to add data to database")


def main():
    stock_analysis_data = get_stock_data()
    if commit_data_to_database(stock_analysis_data, c.server_conn, c.server_name, c.database_name):
        return "Stock data successfully commited to database"


if __name__ == '__main__':
    main()
