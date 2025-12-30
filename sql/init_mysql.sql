CREATE DATABASE IF NOT EXISTS tolldata;

USE tolldata;

CREATE TABLE IF NOT EXISTS livetolldata (
    timestamp DATETIME,
    vehicle_id INT,
    vehicle_type VARCHAR(20),
    toll_plaza_id SMALLINT
);
