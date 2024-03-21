import sqlite3
import pandas as pd

# Connecting database
conn = sqlite3.connect('company_xyz_sales.db')

# Solution using SQL
query_sql = """
            SELECT c.customer_id, c.age, i.item_name, SUM(o.quantity) AS total_quantity
            FROM Customer c
            JOIN Sales s ON c.customer_id = s.customer_id
            JOIN Orders o ON s.sales_id = o.sales_id
            JOIN Items i ON o.item_id = i.item_id
            WHERE c.age BETWEEN 18 AND 35
            GROUP BY c.customer_id, i.item_name
            HAVING total_quantity > 0
            """

# Executing the SQL query and storing the result in a DataFrame
result_sql = pd.read_sql_query(query_sql, conn)

# Storing the query result to a CSV file
result_sql.to_csv('output_sql.csv', index=False)


# =============================================================================================================


# Solution using Pandas
# Getting the necessary data from the database
customer_data = pd.read_sql_query("SELECT * FROM Customer", conn)
sales_data = pd.read_sql_query("SELECT * FROM Sales", conn)
orders_data = pd.read_sql_query("SELECT * FROM Orders", conn)
items_data = pd.read_sql_query("SELECT * FROM Items", conn)

# Merging data
merged_data = pd.merge(pd.merge(pd.merge(customer_data, sales_data, on='customer_id'),
                                orders_data, on='sales_id'),
                       items_data, on='item_id')

# Filtering for customers aged 18-35 and summing the quantities for each item
result_pandas = merged_data[merged_data['age'].between(18, 35)] \
                .groupby(['customer_id', 'age', 'item_name'])['quantity'] \
                .sum().reset_index()

# Filtering out results where total quantity is 0
result_pandas = result_pandas[result_pandas['quantity'] > 0]

# Storing the Pandas result to a CSV file
result_pandas.to_csv('output_pandas.csv', index=False)

# Closing the database connection
conn.close()
