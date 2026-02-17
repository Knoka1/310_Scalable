-- 
-- create the database:
--
USE sys;

DROP DATABASE IF EXISTS photoapp;
CREATE DATABASE photoapp;

-- 
-- now create the tables:
--
USE photoapp;

DROP TABLE IF EXISTS assets;
DROP TABLE IF EXISTS users;

CREATE TABLE users
(
    userid       int not null AUTO_INCREMENT,
    username     varchar(64) not null,
    pwdhash      varchar(256) not null,
    givenname    varchar(64) not null,
    familyname   varchar(64) not null,
    PRIMARY KEY  (userid),
    UNIQUE       (username)
);

ALTER TABLE users AUTO_INCREMENT = 80001;  -- starting value

CREATE TABLE assets
(
    assetid      int not null AUTO_INCREMENT,
    userid       int not null,
    localname    varchar(128) not null,  -- original name from user
    bucketkey    varchar(128) not null,  -- random, unique name in bucket
    PRIMARY KEY (assetid),
    FOREIGN KEY (userid) REFERENCES users(userid),
    UNIQUE      (bucketkey)
);

ALTER TABLE assets AUTO_INCREMENT = 1001;  -- starting value

--
-- Insert some PhotoApp users for our app to start with:
-- 
-- PWD hashing: https://phppasswordhash.com/
--
INSERT INTO users(username, pwdhash, givenname, familyname)  -- pwd = abc123!!
            values('p_sarkar', 
            '$2y$10$/8B5evVyaHF.hxVx0i6dUe2JpW89EZno/VISnsiD1xSh6ZQsNMtXK',
            'Pooja',
            'Sarkar');

INSERT INTO users(username, pwdhash, givenname, familyname)  -- pwd = abc456!!
            values('e_ricci', 
            '$2y$10$F.FBSF4zlas/RpHAxqsuF.YbryKNr53AcKBR3CbP2KsgZyMxOI2z2',
            'Emanuele',
            'Ricci');

INSERT INTO users(username, pwdhash, givenname, familyname)  -- pwd = abc789!!
            values('l_chen', 
            '$2y$10$GmIzRsGKP7bd9MqH.mErmuKvZQ013kPfkKbeUAHxar5bn1vu9.sdK',
            'Li',
            'Chen');
            
--
-- Create ficticious users of photoapp database to limit access:
--
DROP USER IF EXISTS 'photoapp-read-only';
DROP USER IF EXISTS 'photoapp-read-write';

CREATE USER 'photoapp-read-only' IDENTIFIED BY 'abc123!!';
CREATE USER 'photoapp-read-write' IDENTIFIED BY 'def456!!';

GRANT SELECT, SHOW VIEW ON photoapp.* 
      TO 'photoapp-read-only';
GRANT SELECT, SHOW VIEW, INSERT, UPDATE, DELETE, DROP, CREATE, ALTER ON photoapp.* 
      TO 'photoapp-read-write';
      
FLUSH PRIVILEGES;
