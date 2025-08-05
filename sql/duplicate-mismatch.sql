SELECT 
    CTX_name,
    CTX_latitude,
    CTX_longitude,
    PiggyCards_name,
    PiggyCards_latitude,
    PiggyCards_longitude
FROM duplicates
WHERE CTX_name <> PiggyCards_name;
