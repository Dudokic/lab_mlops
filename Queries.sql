USE lego;

-- 1.	Which LEGO themes have the most sets?
-- Identify the themes that feature the highest number of sets to understand their popularity.

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


-- # 2.	What is the average part count per set for each theme?
-- Analyze the complexity of sets within different themes by calculating the average number of parts.

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



-- 3.	How many unique colors are used across all themes?
-- Determine the overall color diversity in LEGO sets by counting unique colors.


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

--  4.	Which sets have the highest part count?
-- Identify the specific sets that are the most complex in terms of the number of parts used.
    SELECT 
        s.set_num,
        s.name,
        s.num_parts
    FROM 
        sets s
    ORDER BY 
        s.num_parts DESC
    LIMIT 10;

-- 5.	How has the number of unique parts used in sets evolved over the years?
-- Analyze trends in the use of unique parts in LEGO sets over time.
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


-- 6.	What is the distribution of sets across different years?
-- Understand how many sets were released each year, which can indicate market activity and trends.
    SELECT 
        year,
        COUNT(set_num) AS num_sets
    FROM 
        sets
    GROUP BY 
        year
    ORDER BY 
        year;

-- 7.	Which parent themes have the highest average number of unique parts per set?
--  Determine which themes are most innovative by looking at the average number of unique parts used in their sets.

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


--  8.	What is the total number of parts used in all sets of  specific theme?
--  Calculate the total part count for a specific theme to assess its material usage.

query = f
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

--  9.	Which themes have experienced the most significant change in average part count over time?
-- Identify themes that have increased or decreased in complexity based on average part counts across years.

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


-- 10.	How do the number of sets and average part count correlate within each theme?
-- Analyze the relationship between the number of sets and the average part count for parent themes to see if more sets mean more complex designs.

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