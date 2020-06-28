-- DDL script for creating Vertica tables and projections

-- Table with main results on each grand prix
CREATE TABLE IF NOT EXISTS gp_results (
    season INT,
    grand_prix VARCHAR(255),
    pos_num INT,
    points FLOAT,
    driver VARCHAR(255),
    driver_nation VARCHAR(255),
    constructor VARCHAR(255),
    constructor_nation VARCHAR(255),
    status VARCHAR(255)
);

-- Table with laptimes on each circuit
CREATE TABLE IF NOT EXISTS circuits_laps (
    circuit VARCHAR(255),
    season INT,
    driver VARCHAR(255),
    time_str VARCHAR(255),
    time_ms INT
);

-- Projections
-- For analysing driver's positions on each grand prix
CREATE PROJECTION IF NOT EXISTS driver_pos_proj (
        driver ENCODING block_dict,
        grand_prix ENCODING  block_dict,
        pos_num
        )
        AS SELECT driver, grand_prix, pos_num
           FROM gp_results
           ORDER BY driver
           SEGMENTED BY HASH(grand_prix) ALL NODES;

-- For analysing constructor's positions on each grand prix
CREATE PROJECTION IF NOT EXISTS constructor_pos_proj (
        constructor ENCODING block_dict,
        grand_prix ENCODING  block_dict,
        pos_num
        )
        AS SELECT constructor, grand_prix, pos_num
           FROM gp_results
           ORDER BY constructor
           SEGMENTED BY HASH(grand_prix) ALL NODES;

-- For analysing driver's points on each grand prix
CREATE PROJECTION IF NOT EXISTS driver_points_proj (
        driver ENCODING block_dict,
        grand_prix ENCODING  block_dict,
        points
        )
        AS SELECT driver, grand_prix, points
           FROM gp_results
           ORDER BY driver
           SEGMENTED BY HASH(grand_prix) ALL NODES;

-- For analysing constructors's finish status on each grand prix
CREATE PROJECTION IF NOT EXISTS constructor_points_proj (
        constructor ENCODING block_dict,
        grand_prix ENCODING  block_dict,
        points
        )
        AS SELECT constructor, grand_prix, points
           FROM gp_results
           ORDER BY constructor
           SEGMENTED BY HASH(grand_prix) ALL NODES;

-- For analysing driver's finish status on each grand prix
CREATE PROJECTION IF NOT EXISTS driver_status_proj (
        constructor ENCODING block_dict,
        grand_prix ENCODING  block_dict,
        status ENCODING block_dict
        )
        AS SELECT constructor, grand_prix, status
           FROM gp_results
           ORDER BY constructor
           SEGMENTED BY HASH(grand_prix) ALL NODES;

-- For analysing driver's finish status on each grand prix
CREATE PROJECTION IF NOT EXISTS constructor_status_proj (
        constructor ENCODING block_dict,
        grand_prix ENCODING  block_dict,
        status ENCODING block_dict
        )
        AS SELECT constructor, grand_prix, status
           FROM gp_results
           ORDER BY constructor
           SEGMENTED BY HASH(grand_prix) ALL NODES;