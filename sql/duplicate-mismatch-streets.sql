SELECT 
    CTX_name,
	CTX_address1,
    CTX_latitude,
    CTX_longitude,
    PiggyCards_name,
	PiggyCards_address1,
    PiggyCards_latitude,
    PiggyCards_longitude
FROM duplicates
WHERE CTX_name <> PiggyCards_name;
