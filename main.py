# Importing required libraries
import sqlparse
import json
import re
import snowflake.connector

# Function to load rules from the JSON file
def load_rules(rules_file):
    # Assuming 1 file
    file = open(rules_file, 'r')
    rules = json.load(file)
    file.close() 
    return rules['rules']

# Function to read the SQL query from user file
def load_sql_query(sql_file):
    file = open(sql_file, 'r')
    query = file.read()
    file.close()
    return query

# Function to check the loaded SQL query against the rules
def analyze(query, rules):
    query_analyzer_output = []
    parsed = sqlparse.parse(query)  # Parse the SQL query using sqlparse
    
    # Iterate through the SQL statements (in case there are multiple)
    for statement in parsed:
        statement_str = str(statement)
        
        # Check each rule against the statement
        for rule in rules:
            if re.search(rule['pattern'], statement_str, re.IGNORECASE):
                query_analyzer_output.append({
                    "id": rule["id"],
                    "description": rule["description"],
                    "recommendation": rule["recommendation"]
                })
    
    return query_analyzer_output

# Function to output analysis results
def calculate_score(query_analyzer_output):
    if not query_analyzer_output:
        score = 10
    else:
        for issue in query_analyzer_output:
            # Write code to calculate score
            continue
    return score

# Real Runtime Analysis with Snowflake -- Have done this before but have to test it

def get_snowflake_connection(user, password, account, warehouse, database, schema):
    """Create a Snowflake connection."""
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    return conn

def query_runtime_results(conn, query):
    # Execute the query which we got from file
    cur = conn.cursor()
    cur.execute(query)
    cur.close()

    # Get QUERY_HISTORY for the query we executed to get performance metrics
    performance_query = """
        SELECT
            query_id,
            execution_status,
            total_elapsed_time,
            total_bytes_scanned,
            total_partitions_scanned,
            query_text,
            warehouse_size,
            warehouse_name,
            start_time,
            end_time,
            user_name
        FROM
            table(information_schema.query_history_by_user())
        WHERE
            query_text = %s
        ORDER BY
            start_time DESC
        LIMIT 1
    """

    cur = conn.cursor()
    cur.execute(performance_query, (query,))
    performance_data = cur.fetchone()
    cur.close()

    if performance_data:
        performance_info = {
            "query_id": performance_data[0],
            "execution_status": performance_data[1],
            "total_elapsed_time": performance_data[2],
            "total_bytes_scanned": performance_data[3],
            "total_partitions_scanned": performance_data[4],
            "warehouse_size": performance_data[6],
            "warehouse_name": performance_data[7],
            "start_time": performance_data[8],
            "end_time": performance_data[9],
            "user_name": performance_data[10]
        }
        return performance_info
    else:
        return None


def main():
    # Load the rules and SQL query file
    rules_file = 'rules.json'  # Path to the JSON rules file
    sql_file = 'query.sql'  # Path to the external SQL query file
    
    rules = load_rules(rules_file)
    query = load_sql_query(sql_file)
    
    # Analyze the SQL query
    query_analyzer_output = analyze(query, rules)
    
    # Display the issues found in query_analyzer_output
    calculate_score(query_analyzer_output)

    # Snowflake credentials will test this block. Did this in Aug 2023 
    conn = get_snowflake_connection(
        user="YOUR_USER",
        password="YOUR_PASSWORD",
        account="YOUR_ACCOUNT",
        warehouse="YOUR_WAREHOUSE",
        database="YOUR_DATABASE",
        schema="YOUR_SCHEMA"
    )
    runtime_results = query_runtime_results(conn, query)

    
    # Close the Snowflake connection
    conn.close()


# Main function
if __name__ == "__main__":
    main()