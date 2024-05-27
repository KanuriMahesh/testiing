import pandas as pd
from prefect import task, Flow
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from prefect.blocks.system import Secret

# Define database connection parameters
SERVER_NAME = Secret.load("server-name").get()
DATABASE_NAME = Secret.load("database-name").get()
USERNAME = Secret.load("username").get()
PASSWORD = Secret.load("password").get()
DRIVER = Secret.load("driver").get()
SCHEMA_NAME = Secret.load("schema-name").get()
TABLE_NAME = Secret.load("table-name").get()

# Create connection string
connection_string = (
    f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER_NAME}:1433/{DATABASE_NAME}?driver={DRIVER}"
)

# Function to read data from a CSV file
@task
def read_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

# Function to connect to Azure SQL Database and create a new table
@task
def create_table(data: pd.DataFrame, table_name: str, schema_name: str):
    try:
        # Connect to the database
        engine = create_engine(connection_string)
        metadata = MetaData(schema=schema_name)

        # Define a new table with the provided name in the specified schema
        table = Table(
            table_name, metadata,
            Column('ID', Integer),
            Column('Name', String(255)),
            schema=schema_name
        )

        # Create the table
        metadata.create_all(engine)

        # Print DataFrame contents
        print("DataFrame Contents:")
        print(data)

        # Insert data into the new table
        with engine.connect() as conn:
            for index, row in data.iterrows():
                insert_query = table.insert().values(ID=row['ID'], Name=row['Name'])
                conn.execute(insert_query)
            
            # Commit the transaction
            conn.commit()

        print(f"Table {schema_name}.{table_name} created and data inserted successfully.")

        # Query data from the table
        query = f"SELECT * FROM {schema_name}.{table_name}"
        table_data = pd.read_sql_query(query, engine)

        # Print the data
        print("Data in Table:")
        print(table_data)

    except Exception as e:
        print(f"Error creating table or inserting data: {e}")

# Wrap the flow definition with @task decorator
@task
def flow_definition():
    # Load data from CSV file
    data = read_csv(r"C:\Users\DD2107\Downloads\MYFILE.csv")

    # Create table and insert data in the specified schema
    create_table(data, TABLE_NAME, SCHEMA_NAME)

# Define the flow within the flow_definition function
with Flow("Load Data to Azure SQL") as flow:
    flow_definition()

# Run the flow
if __name__ == "__main__":
    flow.run()