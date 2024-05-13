CREATE TABLE IF NOT EXISTS universities (
    country         varchar(80),
    alpha_two_code  varchar(2),
    state_province  varchar(100),
    name            varchar(255),
    type            varchar(10),
    
    CONSTRAINT country_university UNIQUE ("alpha_two_code", "name", "state_province")
);