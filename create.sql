-- DROP IF EXISTS
DROP DATABASE IF EXISTS lego;
-- Create the database
CREATE DATABASE lego;

-- Use the created database
USE lego;

-- Create table for part_categories
CREATE TABLE part_categories (
    id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Create table for parts
CREATE TABLE parts (
    part_num VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    part_cat_id INT,
    FOREIGN KEY (part_cat_id) REFERENCES part_categories(id)
);

-- Create table for themes
CREATE TABLE themes (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    parent_id INT NULL  -- Allow NULL values for parent_id
);

-- Create table for colors
CREATE TABLE colors (
    id INT PRIMARY KEY,
    name VARCHAR(255),
    rgb VARCHAR(6),
    is_trans BOOLEAN
);

-- Create table for sets
CREATE TABLE sets (
    set_num VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    year INT,
    theme_id INT,
    num_parts INT,
    FOREIGN KEY (theme_id) REFERENCES themes(id)
);



-- Create table for inventories
CREATE TABLE inventories (
    id INT PRIMARY KEY,
    version INT,
    set_num VARCHAR(50),
    FOREIGN KEY (set_num) REFERENCES sets(set_num)
);

-- Create table for inventory_parts
CREATE TABLE inventory_parts (
    inventory_id INT,
    part_num VARCHAR(50),
    color_id INT,
    quantity INT,
    is_spare BOOLEAN,
    PRIMARY KEY (inventory_id, part_num, color_id),
    FOREIGN KEY (inventory_id) REFERENCES inventories(id),
    FOREIGN KEY (part_num) REFERENCES parts(part_num),
    FOREIGN KEY (color_id) REFERENCES colors(id)
);

-- Create table for inventory_sets
CREATE TABLE inventory_sets (
    inventory_id INT,
    set_num VARCHAR(50),
    quantity INT,
    PRIMARY KEY (inventory_id, set_num),
    FOREIGN KEY (inventory_id) REFERENCES inventories(id),
    FOREIGN KEY (set_num) REFERENCES sets(set_num)
);
