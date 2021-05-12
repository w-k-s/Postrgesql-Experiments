CREATE TABLE IF NOT EXISTS source(
    id serial PRIMARY KEY,
	user_json VARCHAR ( 1000 ) NOT NULL
);

CREATE TABLE IF NOT EXISTS destination(
    id serial PRIMARY KEY,
	first_name VARCHAR ( 1000 ) NOT NULL,
	last_name VARCHAR(100) NOT NULL,
	processed_by VARCHAR(100) NOT NULL
);

INSERT INTO source (user_json) VALUES
('{"firstName":"Harry", "lastName":"Potter"}'),
('{"firstName":"Ron", "lastName":"Weasley"}'),
('{"firstName":"Neville", "lastName":"Longbottom"}'),
('{"firstName":"Albus", "lastName":"Dumbledore"}'),
('{"firstName":"Severus", "lastName":"Snape"}'),
('{"firstName":"Draco", "lastName":"Malfoy"}'),
('{"firstName":"Remus", "lastName":"Lupin"}'),
('{"firstName":"Dean", "lastName":"Thomas"}'),
('{"firstName":"Jack", "lastName":"Torrence"}'),
('{"firstName":"Tom", "lastName":"Riddle"}'),
('{"firstName":"Hermione", "lastName":"Granger"}'),
('{"firstName":"Ginny", "lastName":"Weasley"}'),
('{"firstName":"Minerva", "lastName":"McGonogal"}'),
('{"firstName":"Dolores", "lastName":"Umbridge"}'),
('{"firstName":"Cho", "lastName":"Chang"}'),
('{"firstName":"Lavender", "lastName":"Brown"}'),
('{"firstName":"Belatrix", "lastName":"Lestrange"}'),
('{"firstName":"Narcissa", "lastName":"Malfoy"}'),
('{"firstName":"Nymphodora", "lastName":"Tonks"}'),
('{"firstName":"Jessica", "lastName":"Lupin"}');