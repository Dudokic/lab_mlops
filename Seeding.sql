-- Load data into part_categories
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/part_categories.csv'
INTO TABLE part_categories
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, name);

-- Load data into parts
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/parts.csv'
INTO TABLE parts
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(part_num, name, part_cat_id);


-- Load data into themes
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/themes.csv'
INTO TABLE themes
FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, name, parent_id)
SET parent_id = NULLIF(parent_id, '');

-- Load data into colors
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/colors.csv'
INTO TABLE colors
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, name, rgb, is_trans);


-- Load data into sets
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/sets.csv'
INTO TABLE sets
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(set_num, name, year, theme_id, num_parts);



-- Load data into inventories
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/inventories.csv'
INTO TABLE inventories
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(id, version, set_num);

-- Load data into inventory_parts
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/inventory_parts.csv'
INTO TABLE inventory_parts
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(inventory_id, part_num, color_id, quantity, is_spare);

-- Load data into inventory_sets
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 9.0/Uploads/inventory_sets.csv'
INTO TABLE inventory_sets
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(inventory_id, set_num, quantity);
