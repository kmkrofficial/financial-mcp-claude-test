from mcp.server.fastmcp import FastMCP
import sqlite3
import random
from datetime import datetime, timedelta

mcp = FastMCP("Finance Management Data")
DB_NAME = r'D:\learning\mcp-server\user_transactions.db'

@mcp.tool()
def get_transactions_by_month_and_name(first_name, last_name, month):
    """
    Fetches all transactions for a given person in a specific month.
    Args:
        first_name (str): First name of the person.
        last_name (str): Last name of the person.
        month (int): The month number (1-12).
    Returns:
        list: A list of tuples representing the transactions, or empty list if none found.
              Each tuple contains (transaction_id, transaction_date, amount, location, description).
    """
    conn = sqlite3.connect(DB_NAME)
    # Return rows as dictionary-like objects
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Format month with leading zero if needed (e.g., 1 -> '01')
    month_str = f"{month:02d}"

    cursor.execute('''
        SELECT td.transaction_id, td.transaction_date, td.amount, td.location, td.description
        FROM TransactionData td
        JOIN PeopleInformation pi ON td.person_id = pi.person_id
        WHERE pi.first_name = ? AND pi.last_name = ?
          AND strftime('%m', td.transaction_date) = ?
        ORDER BY td.transaction_date
    ''', (first_name, last_name, month_str))

    transactions = cursor.fetchall()
    conn.close()
    # Convert Row objects to standard dictionaries for easier use
    return [dict(row) for row in transactions]

@mcp.tool()
def get_transactions_by_day_and_name(first_name, last_name, date_str):
    """
    Fetches all transactions for a given person on a specific day.
    Args:
        first_name (str): First name of the person.
        last_name (str): Last name of the person.
        date_str (str): The date string in 'YYYY-MM-DD' format.
    Returns:
        list: A list of tuples representing the transactions, or empty list if none found.
              Each tuple contains (transaction_id, transaction_date, amount, location, description).
    """
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Validate date format (basic check)
    try:
        datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        print("Error: Invalid date format. Please use YYYY-MM-DD.")
        conn.close()
        return []

    cursor.execute('''
        SELECT td.transaction_id, td.transaction_date, td.amount, td.location, td.description
        FROM TransactionData td
        JOIN PeopleInformation pi ON td.person_id = pi.person_id
        WHERE pi.first_name = ? AND pi.last_name = ?
          AND date(td.transaction_date) = ?
        ORDER BY td.transaction_date
    ''', (first_name, last_name, date_str))

    transactions = cursor.fetchall()
    conn.close()
    return [dict(row) for row in transactions]

@mcp.tool()
def get_total_transaction_amount_by_name(first_name, last_name):
    """
    Calculates the total amount spent in transactions by a given person.
    Args:
        first_name (str): First name of the person.
        last_name (str): Last name of the person.
    Returns:
        float: The total amount spent, or 0.0 if the person or transactions are not found.
    """
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('''
        SELECT SUM(td.amount)
        FROM TransactionData td
        JOIN PeopleInformation pi ON td.person_id = pi.person_id
        WHERE pi.first_name = ? AND pi.last_name = ?
    ''', (first_name, last_name))

    result = cursor.fetchone()
    conn.close()

    # fetchone() returns a tuple (total,) or (None,) if no matching rows
    if result and result[0] is not None:
        return round(result[0], 2)
    else:
        return 0.0

@mcp.tool()
def list_all_people():
    """
    Fetches a list of all people in the system, including their
    most frequent transaction location.
    Returns:
        list: A list of dictionaries, each representing a person.
              Keys: 'person_id', 'first_name', 'last_name', 'email',
                    'phone_number', 'most_frequent_location' (can be None).
    """
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row # Return rows as dictionary-like objects
    cursor = conn.cursor()

    # Use a CTE to rank locations by frequency for each person
    # Tie-breaking: If counts are equal, pick the one with the most recent transaction
    sql_query = """
    WITH RankedLocations AS (
        SELECT
            person_id,
            location,
            ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY COUNT(*) DESC, MAX(transaction_date) DESC) as rn
        FROM TransactionData
        WHERE location IS NOT NULL AND location != '' -- Ignore potentially empty/null locations
        GROUP BY person_id, location
    )
    SELECT
        pi.person_id,
        pi.first_name,
        pi.last_name,
        pi.email,
        pi.phone_number,
        rl.location AS most_frequent_location
    FROM PeopleInformation pi
    LEFT JOIN RankedLocations rl ON pi.person_id = rl.person_id AND rl.rn = 1 -- Get only the top ranked location (rn=1)
    ORDER BY pi.last_name, pi.first_name;
    """

    cursor.execute(sql_query)
    people = cursor.fetchall()
    conn.close()

    # Convert Row objects to standard dictionaries
    return [dict(row) for row in people]


@mcp.tool()
def get_transactions_by_location(location):
    """
    Fetches all transactions that occurred at a specific location.
    Args:
        location (str): The location name to filter by.
    Returns:
        list: A list of dictionaries representing the transactions including person's name.
              Keys: 'transaction_id', 'first_name', 'last_name', 'transaction_date',
                    'amount', 'location', 'description'
    """
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            td.transaction_id,
            pi.first_name,
            pi.last_name,
            td.transaction_date,
            td.amount,
            td.location,
            td.description
        FROM TransactionData td
        JOIN PeopleInformation pi ON td.person_id = pi.person_id
        WHERE td.location = ?
        ORDER BY td.transaction_date DESC
    ''', (location,))

    transactions = cursor.fetchall()
    conn.close()
    return [dict(row) for row in transactions]

if __name__ == "__main__":
    POSSIBLE_LOCATIONS = ["New York", "London", "Tokyo", "Paris", "Berlin", "Sydney", "Mumbai", "Online", "Arcot", "Chennai"]
    # # 1. Setup the database and tables
    # setup_database()

    # # 2. Populate with dummy data (only if empty)
    # populate_dummy_data()

    print("\n--- Database Operations Demo ---")

    # 3. List all people (NOW INCLUDES LOCATION)
    print("\nListing all people in the system (with most frequent location):")
    all_people = list_all_people()
    if all_people:
        for person in all_people[:15]: # Print more people to see variety
            location_str = person['most_frequent_location'] if person['most_frequent_location'] else "N/A"
            print(f"  ID: {person['person_id']}, Name: {person['first_name']} {person['last_name']}, "
                  f"Email: {person['email']}, Location: {location_str}")
        # if len(all_people) > 15: # Adjust if needed
        #     print(f"  ... and {len(all_people) - 15} more.")

        # Select a random person for further demos (if people exist)
        if all_people:
             demo_person = random.choice(all_people)
             demo_first_name = demo_person['first_name']
             demo_last_name = demo_person['last_name']
             print(f"\n--- Using '{demo_first_name} {demo_last_name}' for specific demos ---")

             # ... (rest of the demo remains the same) ...

             # 4. Get transactions by month and name
             demo_month = random.randint(1, 12)
             print(f"\nTransactions for {demo_first_name} {demo_last_name} in Month {demo_month}:")
             month_transactions = get_transactions_by_month_and_name(demo_first_name, demo_last_name, demo_month)
             if month_transactions:
                 for tx in month_transactions:
                     print(f"  - {tx['transaction_date']} | ${tx['amount']:.2f} at {tx['location']} ({tx['description']})")
             else:
                 print("  No transactions found for this month.")

             # 5. Get transactions by day and name
             demo_date = ""
             if month_transactions:
                 try:
                    first_tx_date_str = month_transactions[0]['transaction_date']
                    if '.' in first_tx_date_str: first_tx_date_str = first_tx_date_str.split('.')[0]
                    demo_date = datetime.strptime(first_tx_date_str, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d')
                 except (ValueError, KeyError, IndexError):
                      demo_date = (datetime.now() - timedelta(days=random.randint(10, 60))).strftime('%Y-%m-%d')
             else:
                  demo_date = (datetime.now() - timedelta(days=random.randint(10, 60))).strftime('%Y-%m-%d')

             print(f"\nTransactions for {demo_first_name} {demo_last_name} on Day {demo_date}:")
             day_transactions = get_transactions_by_day_and_name(demo_first_name, demo_last_name, demo_date)
             if day_transactions:
                 for tx in day_transactions:
                      print(f"  - {tx['transaction_date']} | ${tx['amount']:.2f} at {tx['location']} ({tx['description']})")
             else:
                 print("  No transactions found for this specific day.")

             # 6. Get total transaction amount by name
             total_amount = get_total_transaction_amount_by_name(demo_first_name, demo_last_name)
             print(f"\nTotal transaction amount for {demo_first_name} {demo_last_name}: ${total_amount:.2f}")

    else:
        print("No people found in the database. Cannot run demos.")

    # 7. Get transactions by location
    demo_location = random.choice(POSSIBLE_LOCATIONS)
    print(f"\nTransactions at location '{demo_location}':")
    location_transactions = get_transactions_by_location(demo_location)
    if location_transactions:
        for tx in location_transactions[:5]: # Show first 5
            print(f"  - {tx['transaction_date']} | {tx['first_name']} {tx['last_name']} | ${tx['amount']:.2f} ({tx['description']})")
        if len(location_transactions) > 5:
             print(f"  ... and {len(location_transactions) - 5} more.")
    else:
        print(f"  No transactions found for location '{demo_location}'.")

    print("\n--- Demo Complete ---")
    mcp.run()
