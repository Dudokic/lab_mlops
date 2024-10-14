import pandas as pd
import numpy as np
from IPython.display import display

#Importing the datainto data frames
colors = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/colors.csv")
inventories = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/inventories.csv")
inventory_parts = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/inventory_parts.csv")
inventory_sets = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/inventory_sets.csv")
part_categories = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/part_categories.csv")
parts = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/parts.csv")
sets = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/sets.csv")
themes = pd.read_csv("C:/Users/dusan/Documents/GitHub/project-2-eda-sql/themes.csv")


# Understanding the data

print(colors.head())