# Importing required libraries
import sqlparse
import sqlfluff
import json
import re
import snowflake.connector

# Function to load analysis rules from the JSON file
def load_analysis_rules(rules_file):
    # Assuming 1 file
    file = open(rules_file, 'r')
    rules = json.load(file)
    file.close() 
    return rules['rules']

# Function to load Syntax rules from the JSON file
def load_syntax_rules(rules_file):
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

# Function to check the loaded SQL query against the syntax rules
def sql_syntax_check(query):
    
    # Use this to identify syntax issues
    
    return None

# Function to output syntax results
def calculate_syntax_score(query_issues):
    # Use this to calculate Syntax score    
    return None


# Function to check the loaded SQL query against the analysis rules
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
                    "recommendation": rule["recommendation"],
                    #Add suggested fix then and there
                    #"suggestion": suggested_fix(rule["id"], statement_str)
                })
    
    return query_analyzer_output

# Function to output analysis results
def calculate_query_score(query_issues):
    if not query_issues:
        score = 10
    else:
        for issue in query_issues:
            # Write code to calculate score
            continue
    return score

"""
def suggested_fix(issue_id, part_of_query):
    #Here we can create logic to return a fix based on suggestions
    if issue_id == "xyz":
        # Suggest replacing SELECT * with explicit column names (assumes column metadata is available)
        return the same value

    # Other potential suggestions to be explored
    return None
"""

# After Static analysis we do Real Runtime Analysis with Snowflake -- Have done this before but have to test it

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

    # If performance metrics exists we will save it in a variable else we return a blank
    # - Have to see how to deal with Blanks
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
    
def analyze_performance(performance_info):

    # We can define static baselines seaching online
    baseline_elapsed_time = 123  # This if for Time
    baseline_bytes_scanned = 1234567  # This is for size

    performance_issues = []
    if performance_info['total_elapsed_time'] > baseline_elapsed_time:
        performance_issues.append({
            "issue": "Time exceeded",
            "recommendation": "xyz"
        })

    if performance_info['total_bytes_scanned'] > baseline_bytes_scanned:
        performance_issues.append({
            "issue": "It is scanning full table",
            "recommendation": "Add some filter or where clause"
        })
    return performance_issues

# Function to output performance results
def calculate_performance_score(performance_issues):
    if not performance_issues:
        score = 10
    else:
        for issue in performance_issues:
            # Write code to calculate score
            continue
    return score





def main():
    # Load the rules and SQL query file
    analysis_rules_file = 'analysis_rules.json'  # Path to the Analysis JSON rules file
    syntax_rules_file = 'syntax_rules.json'  # Path to the Syntax JSON rules file
    sql_file = 'query.sql'  # Path to the external SQL query file
    
    analysis_rules = load_analysis_rules(analysis_rules_file)
    syntax_rules = load_syntax_rules(syntax_rules_file)
    query = load_sql_query(sql_file)
    
    # Analyze the SQL query for analytical errors
    query_analyzer_output = analyze(query, analysis_rules)
    
    # Display the issues found in query_analyzer_output
    analysis_query_score = calculate_query_score(query_analyzer_output)

    # Analyze the SQL query for syntax
    syntax_analyzer_output = sql_syntax_check(query, syntax_rules)
    
    # Display the issues found in query_analyzer_output
    syntax_query_score = calculate_syntax_score(syntax_analyzer_output)

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
    performance_analyzer_output = analyze_performance(runtime_results)
    performance_score = calculate_performance_score(performance_analyzer_output)


    print(performance_analyzer_output)
    print(analysis_query_score)
    print(syntax_query_score)
    
    # Close the Snowflake connection
    conn.close()


# Main function
if __name__ == "__main__":
    main()