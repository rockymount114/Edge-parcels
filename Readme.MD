## Fetch the latest parcel data from Edge county website


## Fetch data from TS, Munis db for data cleaning

1. Nash parcels

TXSDIST IN ('RM00', 'RM62')

IF OWNER1 = OWNER2, empty OWNER2

ML_C_ST_Z split into CITY, STATE, ZIPC columns

IF MAIL_ADDR1 LIKE '%MAIL RETURNED%' using Munis parcel or customer information to replace MAIL_ADDR1, ADDR2, CITY, STATE and ZIPCODE
