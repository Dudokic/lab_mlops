# %%
import pandas as pd
import numpy as np
from IPython.display import display
import matplotlib.pyplot as plt
import seaborn as sns

# %%
#Importing the datainto data frames
datasets = {
    "colors": pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/colors.csv"),
    "inventories": pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/inventories.csv"),
    "inventory_parts": pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/inventory_parts.csv"),
    "inventory_sets": pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/inventory_sets.csv"),
    "part_categories": pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/part_categories.csv"),
    "parts": pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/parts.csv"),
    "sets": pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/sets.csv"),
    "themes": pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/themes.csv")
}

# %%
colors = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/colors.csv")
inventories = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/inventories.csv")
inventory_parts = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/inventory_parts.csv")
inventory_sets = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/inventory_sets.csv")
part_categories = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/part_categories.csv")
parts = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/parts.csv")
sets = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/sets.csv")
themes = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/themes.csv")

# %% [markdown]
# # EDA
# ## STEP 1: Looking at the data and identifying missing Data

# %%
# Loop through each dataset and display the required information
for name, df in datasets.items():
    print(f"\n--- {name} Dataset Overview ---")
    
    # Info
    df.info()
    
    # Shape
    print("\nShape of the DataFrame:")
    display(df.shape)
    
    # First 10 rows
    print("\nFirst 10 rows:")
    display(df.head(10))
    
    # Missing values
    print("\nMissing values per column:")
    display(df.isnull().sum())
    
    # Column names
    print("\nColumn names:")
    display(df.columns)

# %% [markdown]
# # RESULT:Empty parent_id Values in the themes.csv File

# %% [markdown]
# ## STEP 2: Data Cleaning

# %%
for name, df in datasets.items():
    print(f"\n--- {name} Dataset Overview ---")
    
    # First 10 rows
    print("\nFirst 10 rows:")
    display(df.head(10))


# %% [markdown]
# In the Colors Table there is a color with the name "Unknown" since there is RGB code i was able to identify this color as mainly Blue. Therefore we will rename the color into "Blue2"

# %%
colors["name"] = colors["name"].replace({"Unknown": "Blue2"})

# %%
colors.head()

# %% [markdown]
# # Step 3: Univariate Analysis

# %% [markdown]
# ## Table: Sets

# %%
sets.head()


# %%
sets["name"].value_counts()

# %%
# Calculate value counts
name_counts = sets['name'].value_counts()

# Select the top 10 most frequent set names
top_n = 10
top_names = name_counts.head(top_n).index.tolist()

# Filter the DataFrame to include only the top set names
filtered_sets = sets[sets['name'].isin(top_names)].copy()

# Group the data by 'year' and 'name' and count occurrences
grouped_counts = filtered_sets.groupby(['year', 'name']).size().reset_index(name='counts')

# Pivot the data to have years as index and set names as columns
pivot_df = grouped_counts.pivot(index='year', columns='name', values='counts').fillna(0)

# Ensure the years are sorted
pivot_df = pivot_df.sort_index()


# Plot the data

plt.figure(figsize=(15, 7))
for column in pivot_df.columns:
    plt.plot(pivot_df.index, pivot_df[column], marker='o', label=column)

plt.xlabel('Year')
plt.ylabel('Number of Times Produced')
plt.title('Frequency of Top LEGO Sets Produced Over the Years')
plt.legend(title='Set Name', bbox_to_anchor=(1.05, 1), loc='upper left')
plt.grid(True)
plt.tight_layout()
plt.show()

# %%
avg_parts_per_year = sets.groupby("year")["num_parts"].mean()

plt.figure(figsize=(12, 6))
plt.plot(avg_parts_per_year.index, avg_parts_per_year.values, marker='o')
plt.xlabel('Year')
plt.ylabel('Average Number of Parts')
plt.title('Average Number of Parts in LEGO Sets Over the Years')
plt.grid(True)
plt.show()

# %%
#Analyzing the Lifecycle of Sets

# Calculate the first and last year each set name was produced
lifecycle = filtered_sets.groupby('name')['year'].agg(['min', 'max']).reset_index()
lifecycle['years_in_production'] = lifecycle['max'] - lifecycle['min'] + 1

# Display the lifecycle DataFrame
print(lifecycle)

# %%
plt.figure(figsize=(10, 6))
plt.barh(lifecycle['name'], lifecycle['years_in_production'])
plt.xlabel('Years in Production')
plt.ylabel('Set Name')
plt.title('Lifespan of Top LEGO Sets')
plt.tight_layout()
plt.show()

# %% [markdown]
# ## Table: Themes
# 

# %%
themes.head()

# %%
themes_value_count = themes["name"].value_counts()
display(themes_value_count)

# %% [markdown]
# # Bivariate Analysis: Sets & Themes

# %%
themes.head()

# %%
filtered_sets.head()

# %%
# Merge the filtered sets with themes
sets_with_themes = pd.merge(sets, themes, left_on='theme_id', right_on='id', how='left', suffixes=('_set', '_theme'))


# %%
# Analyze the themes of top sets
theme_counts = sets_with_themes['name_theme'].value_counts()

# Display the top themes
print(theme_counts)

# %% [markdown]
# # SQL QUERIES

# %%

# %%
import mysql.connector
from mysql.connector import Error

# Reuse connection and cursor
connection = None
cursor = None

# Function to create the connection once
def create_connection():
    global connection, cursor  # Make them accessible in the entire program
    try:
        if not connection:  # Only connect if connection is not already established
            connection = mysql.connector.connect(
                host='localhost',
                user='root',
                password='Apfelsaft_1',
                database='lego'  # Your existing database
            )
            if connection.is_connected():
                cursor = connection.cursor()  # Create the cursor once
                print("Successfully connected to MySQL")
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")


# %%
# Function to execute any query
def execute_query(query, values=None, fetch=False):
    try:
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)

        if fetch:
            return cursor.fetchall()
    except Error as e:
        print(f"Error executing query: {e}")
        return None

# %%
# Function to drop and create the database
def create_database(cursor):
    try:
        cursor.execute("DROP DATABASE IF EXISTS lego")  # Drop database if it exists
        cursor.execute("CREATE DATABASE lego")  # Create a fresh database
        print("Database 'lego' recreated")
    except Error as e:
        print(f"Error while creating the database: {e}")

# %%
def create_tables(cursor):
    try:
        cursor.execute("USE lego")
        
        # Create table for part_categories
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS part_categories (
                id INT PRIMARY KEY,
                name VARCHAR(255) NOT NULL
            )
        ''')
        
        # Create table for parts
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS parts (
                part_num VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255),
                part_cat_id INT,
                FOREIGN KEY (part_cat_id) REFERENCES part_categories(id)
            )
        ''')
        
        # Create table for themes
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS themes (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                parent_id INT NULL
            )
        ''')

        # Create table for colors
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS colors (
                id INT PRIMARY KEY,
                name VARCHAR(255),
                rgb VARCHAR(6),
                is_trans BOOLEAN
            )
        ''')

        # Create table for sets
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sets (
                set_num VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255),
                year INT,
                theme_id INT,
                num_parts INT,
                FOREIGN KEY (theme_id) REFERENCES themes(id)
            )
        ''')

        # Create table for inventories
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventories (
                id INT PRIMARY KEY,
                version INT,
                set_num VARCHAR(50),
                FOREIGN KEY (set_num) REFERENCES sets(set_num)
            )
        ''')

        # Create table for inventory_parts
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory_parts (
                inventory_id INT,
                part_num VARCHAR(50),
                color_id INT,
                quantity INT,
                is_spare BOOLEAN,
                PRIMARY KEY (inventory_id, part_num, color_id),
                FOREIGN KEY (inventory_id) REFERENCES inventories(id),
                FOREIGN KEY (part_num) REFERENCES parts(part_num),
                FOREIGN KEY (color_id) REFERENCES colors(id)
            )
        ''')

        # Create table for inventory_sets
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS inventory_sets (
                inventory_id INT,
                set_num VARCHAR(50),
                quantity INT,
                PRIMARY KEY (inventory_id, set_num),
                FOREIGN KEY (inventory_id) REFERENCES inventories(id),
                FOREIGN KEY (set_num) REFERENCES sets(set_num)
            )
        ''')
        
        print("Tables created successfully")
        
    except Error as e:
        print(f"Error while creating tables: {e}")

# %%
# Function to load data using LOAD DATA INFILE
def load_data(cursor, connection):
    try:
        # Disable foreign key checks to speed up data loading
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        
        # Load data into part_categories
        cursor.execute('''
            LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/part_categories.csv'
            INTO TABLE part_categories
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (id, name);
        ''')
        
        # Load data into parts
        cursor.execute('''
            LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/parts.csv'
            INTO TABLE parts
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (part_num, name, part_cat_id);
        ''')

        # Load data into themes
        cursor.execute('''
            LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/themes.csv'
            INTO TABLE themes
            FIELDS TERMINATED BY ',' 
            OPTIONALLY ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (id, name, parent_id)
            SET parent_id = NULLIF(parent_id, '');
        ''')

        # Load data into colors
        cursor.execute('''
            LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/colors.csv'
            INTO TABLE colors
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (id, name, rgb, is_trans);
        ''')

        # Load data into sets
        cursor.execute('''
            LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/sets.csv'
            INTO TABLE sets
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (set_num, name, year, theme_id, num_parts);
        ''')

        # Load data into inventories
        cursor.execute('''
            LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/inventories.csv'
            INTO TABLE inventories
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (id, version, set_num);
        ''')

        # Load data into inventory_parts
        cursor.execute('''
            LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/inventory_parts.csv'
            INTO TABLE inventory_parts
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (inventory_id, part_num, color_id, quantity, is_spare);
        ''')

        # Load data into inventory_sets
        cursor.execute('''
            LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/inventory_sets.csv'
            INTO TABLE inventory_sets
            FIELDS TERMINATED BY ',' 
            ENCLOSED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 LINES
            (inventory_id, set_num, quantity);
        ''')

        # Enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")

        # Commit the transactions
        connection.commit()
        print("Data inserted successfully")
        
    except Error as e:
        print(f"Error while inserting data: {e}")
        connection.rollback()  # Rollback in case of error


# %%
# Main function to execute everything
def main():
    connection = create_connection()
    if connection:
        cursor = connection.cursor()
        create_database(cursor)
        create_tables(cursor)
        load_data(cursor,connection)
        cursor.close()
        connection.close()

if __name__ == "__main__":
    main()

# %% [markdown]
# 
# # 1.	Which LEGO themes have the most sets?
# ## Identify the themes that feature the highest number of sets to understand their popularity.
# 
# 

# %%
import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',  # Your MySQL server address
    user='root',  # Your MySQL username
    password='Apfelsaft_1',  # Your MySQL password
    db='lego',  # The database you're working with
    cursorclass=pymysql.cursors.DictCursor
)

# Query to get the number of sets per LEGO theme, specifically for Star Wars themes
query = """
    SELECT 
        t.name AS theme_name,
        COUNT(s.set_num) AS num_sets
    FROM 
        sets s
    JOIN 
        themes t ON s.theme_id = t.id
    WHERE 
        t.name LIKE '%Star Wars%'  -- Adjust the condition to match your theme naming convention
    GROUP BY 
        t.name
    ORDER BY 
        num_sets DESC;
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_most_sets = pd.DataFrame(result)

# Visualization
plt.figure(figsize=(10, 6))
plt.barh(df_most_sets['theme_name'], df_most_sets['num_sets'], color='skyblue')
plt.title('Number of Sets per LEGO Star Wars Theme')
plt.xlabel('Number of Sets')
plt.ylabel('Theme Name')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %% [markdown]
# # 2.	What is the average part count per set for each theme?
# ## Analyze the complexity of sets within different themes by calculating the average number of parts.
# 

# %%
import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',  # Your MySQL server address
    user='root',  # Your MySQL username
    password='Apfelsaft_1',  # Your MySQL password
    db='lego',  # The database you're working with
    cursorclass=pymysql.cursors.DictCursor
)

# Query to get the average part count per LEGO Star Wars themes
query = """
    SELECT 
        t.name AS theme_name,
        AVG(s.num_parts) AS avg_part_count
    FROM 
        sets s
    JOIN 
        themes t ON s.theme_id = t.id
    WHERE 
        t.name LIKE '%Star Wars%'  -- Adjust the condition to match your theme naming convention
    GROUP BY 
        t.name;
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_avg_part_count = pd.DataFrame(result)

# Visualization
plt.figure(figsize=(10, 6))
plt.barh(df_avg_part_count['theme_name'], df_avg_part_count['avg_part_count'], color='lightgreen')
plt.title('Average Part Count per LEGO Star Wars Theme')
plt.xlabel('Average Part Count')
plt.ylabel('Theme Name')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %% [markdown]
# # 3.	How many unique colors are used across all themes?
# ## Determine the overall color diversity in LEGO sets by counting unique colors.
# 

# %%
import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',  # Your MySQL server address
    user='root',  # Your MySQL username
    password='Apfelsaft_1',  # Your MySQL password
    db='lego',  # The database you're working with
    cursorclass=pymysql.cursors.DictCursor
)

# Query to get the count of unique colors used in each Star Wars theme
query = """
    SELECT 
        t.name AS theme_name,
        COUNT(DISTINCT c.id) AS unique_colors
    FROM 
        colors c
    JOIN 
        inventory_parts ip ON c.id = ip.color_id
    JOIN 
        inventories i ON ip.inventory_id = i.id
    JOIN 
        sets s ON i.set_num = s.set_num
    JOIN 
        themes t ON s.theme_id = t.id
    WHERE 
        t.name LIKE '%Star Wars%'
    GROUP BY 
        t.name;
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_unique_colors_per_theme = pd.DataFrame(result)

# Visualization
plt.figure(figsize=(10, 6))
plt.barh(df_unique_colors_per_theme['theme_name'], df_unique_colors_per_theme['unique_colors'], color='lightblue')
plt.title('Unique Colors Count per LEGO Star Wars Theme')
plt.xlabel('Number of Unique Colors')
plt.ylabel('Theme Name')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %% [markdown]
# # 4.	Which sets have the highest part count?
# ## Identify the specific sets that are the most complex in terms of the number of parts used.
# 

# %%
import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',  # Your MySQL server address
    user='root',  # Your MySQL username
    password='Apfelsaft_1',  # Your MySQL password
    db='lego',  # The database you're working with
    cursorclass=pymysql.cursors.DictCursor
)

# Query to get the sets with the highest part count
query = """
    SELECT 
        s.set_num,
        s.name,
        s.num_parts
    FROM 
        sets s
    ORDER BY 
        s.num_parts DESC
    LIMIT 10;
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_highest_part_count = pd.DataFrame(result)

# Display result
print('Top 10 Sets with Highest Part Count:')
print(df_highest_part_count)

# Visualization
plt.figure(figsize=(12, 8))
plt.barh(df_highest_part_count['name'], df_highest_part_count['num_parts'], color='cornflowerblue')
plt.title('Top 10 LEGO Sets with Highest Part Count')
plt.xlabel('Number of Parts')
plt.ylabel('Set Name')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %% [markdown]
# # 5.	How has the number of unique parts used in sets evolved over the years?
# ## Analyze trends in the use of unique parts in LEGO sets over time.
# 

# %%
# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Apfelsaft_1',
    db='lego',
    cursorclass=pymysql.cursors.DictCursor
)

# Query to get unique parts evolution over the years
query = """
    SELECT 
        s.year,
        COUNT(DISTINCT ip.part_num) AS unique_parts
    FROM 
        sets s
    JOIN 
        inventories i ON s.set_num = i.set_num
    JOIN 
        inventory_parts ip ON i.id = ip.inventory_id
    GROUP BY 
        s.year
    ORDER BY 
        s.year;
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_unique_parts_evolution = pd.DataFrame(result)

# Visualization
plt.figure(figsize=(10, 6))
plt.plot(df_unique_parts_evolution['year'], df_unique_parts_evolution['unique_parts'], marker='o')
plt.title('Evolution of Unique Parts Used Over the Years')
plt.xlabel('Year')
plt.ylabel('Number of Unique Parts')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %% [markdown]
# # 6.	What is the distribution of sets across different years?
# ## Understand how many sets were released each year, which can indicate market activity and trends.
# 

# %%
# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',
    user='root',
    password='Apfelsaft_1',
    db='lego',
    cursorclass=pymysql.cursors.DictCursor
)

# Query to get the distribution of sets across different years
query = """
    SELECT 
        year,
        COUNT(set_num) AS num_sets
    FROM 
        sets
    GROUP BY 
        year
    ORDER BY 
        year;
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_sets_distribution = pd.DataFrame(result)

# Visualization
plt.figure(figsize=(10, 6))
plt.plot(df_sets_distribution['year'], df_sets_distribution['num_sets'], marker='o', color='orange')
plt.title('Number of Sets Released Across Years')
plt.xlabel('Year')
plt.ylabel('Number of Sets')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %% [markdown]
# # 7.	Which parent themes have the highest average number of unique parts per set?
# ## Determine which themes are most innovative by looking at the average number of unique parts used in their sets.
# 

# %%
import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',  # Your MySQL server address
    user='root',  # Your MySQL username
    password='Apfelsaft_1',  # Your MySQL password
    db='lego',  # The database you're working with
    cursorclass=pymysql.cursors.DictCursor
)

# Query to get average unique parts per parent theme, limited to top 20
query = """
    SELECT 
        t.parent_id,
        pt.name AS parent_theme_name,
        AVG(unique_parts) AS avg_unique_parts
    FROM (
        SELECT 
            s.theme_id,
            COUNT(DISTINCT ip.part_num) AS unique_parts
        FROM 
            sets s
        JOIN 
            inventories i ON s.set_num = i.set_num
        JOIN 
            inventory_parts ip ON i.id = ip.inventory_id
        GROUP BY 
            s.set_num
    ) AS unique_parts_per_set
    JOIN 
        themes t ON unique_parts_per_set.theme_id = t.id
    JOIN 
        themes pt ON t.parent_id = pt.id  -- Joining to get the parent theme name
    GROUP BY 
        t.parent_id, pt.name
    ORDER BY 
        avg_unique_parts DESC  -- Order by average unique parts in descending order
    LIMIT 20;  -- Limit to top 20 results
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_avg_unique_parts_per_parent = pd.DataFrame(result)

# Visualization
plt.figure(figsize=(12, 8))
plt.barh(df_avg_unique_parts_per_parent['parent_theme_name'], df_avg_unique_parts_per_parent['avg_unique_parts'], color='lightcoral')
plt.title('Top 20 Parent LEGO Themes by Average Unique Parts')
plt.xlabel('Average Unique Parts')
plt.ylabel('Parent Theme Name')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %% [markdown]
# # 8.	What is the total number of parts used in all sets of a specific theme?
# ## Calculate the total part count for a specific theme to assess its material usage.
# 

# %%
import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',  # Your MySQL server address
    user='root',  # Your MySQL username
    password='Apfelsaft_1',  # Your MySQL password
    db='lego',  # The database you're working with
    cursorclass=pymysql.cursors.DictCursor
)

# List of themes to compare
themes_to_compare = ["Harry Potter", "Star Wars", "Avatar","Spider-Man", "Bionicle"]  # Add other themes as needed
themes_placeholder = ', '.join([f"'{theme}'" for theme in themes_to_compare])

# Query to get total parts for specified themes
query = f"""
    SELECT 
        t.name AS theme_name,
        SUM(s.num_parts) AS total_parts
    FROM 
        sets s
    JOIN 
        themes t ON s.theme_id = t.id
    WHERE 
        t.name IN ({themes_placeholder})
    GROUP BY 
        t.name;
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_total_parts = pd.DataFrame(result)

# Display result
print(df_total_parts)

# Visualization
plt.figure(figsize=(10, 6))
plt.barh(df_total_parts['theme_name'], df_total_parts['total_parts'], color='lightblue')
plt.title('Total Parts in Selected LEGO Themes')
plt.xlabel('Total Parts')
plt.ylabel('Theme Name')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %% [markdown]
# # 9.	Which themes have experienced the most significant change in average part count over time?
# ## Identify themes that have increased or decreased in complexity based on average part counts across years.
# 

# %%
import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',  # Your MySQL server address
    user='root',  # Your MySQL username
    password='Apfelsaft_1',  # Your MySQL password
    db='lego',  # The database you're working with
    cursorclass=pymysql.cursors.DictCursor
)

# Query to get average part count over the years by theme
query = """
    SELECT 
        t.name AS theme_name,
        s.year,
        AVG(s.num_parts) AS avg_part_count
    FROM 
        sets s
    JOIN 
        themes t ON s.theme_id = t.id
    GROUP BY 
        t.name, s.year
    ORDER BY 
        t.name, s.year;
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_avg_part_count_change = pd.DataFrame(result)

# Calculate change in average part count for each theme
change_df = df_avg_part_count_change.groupby('theme_name').agg(
    avg_change=('avg_part_count', lambda x: x.iloc[-1] - x.iloc[0])  # Calculate change from first to last year
).reset_index()

# Convert avg_change to numeric type
change_df['avg_change'] = pd.to_numeric(change_df['avg_change'], errors='coerce')

# Get the top 10 themes with the highest change in average part count
top_10_themes = change_df.nlargest(10, 'avg_change')

# Visualization
plt.figure(figsize=(10, 6))
plt.barh(top_10_themes['theme_name'], top_10_themes['avg_change'], color='lightgreen')
plt.title('Top 10 LEGO Themes by Change in Average Part Count')
plt.xlabel('Change in Average Part Count')
plt.ylabel('Theme Name')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %% [markdown]
# # 10.	How do the number of sets and average part count correlate within each theme?
# ## Analyze the relationship between the number of sets and the average part count for themes to see if more sets mean more complex designs.
# 

# %%
import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# Connect to the MySQL database
connection = pymysql.connect(
    host='localhost',  # Your MySQL server address
    user='root',  # Your MySQL username
    password='Apfelsaft_1',  # Your MySQL password
    db='lego',  # The database you're working with
    cursorclass=pymysql.cursors.DictCursor
)

# Query to analyze correlation between number of sets and average part count per overall theme
query = """
    SELECT 
        t.parent_id,
        t.name AS theme_name,
        COUNT(s.set_num) AS num_sets,
        AVG(s.num_parts) AS avg_part_count
    FROM 
        sets s
    JOIN 
        themes t ON s.theme_id = t.id
    GROUP BY 
        t.parent_id, t.name;
"""

# Fetch data
with connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

# Convert to DataFrame
df_correlation = pd.DataFrame(result)

# Visualization
plt.figure(figsize=(10, 6))
plt.scatter(df_correlation['num_sets'], df_correlation['avg_part_count'], color='purple')
plt.title('Sets vs. Average Part Count Correlation (Overall Themes)')
plt.xlabel('Number of Sets')
plt.ylabel('Average Part Count')
plt.tight_layout()
plt.show()

# Close the connection
connection.close()


# %%



