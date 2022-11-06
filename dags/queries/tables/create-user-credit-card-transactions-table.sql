CREATE OR REPLACE TABLE TEST.APP."UserCreditCardTransactions" (
    "User" NUMBER(38,0),
    "Card" NUMBER(38,0),
    "Year" NUMBER(38,0),
    "Month" NUMBER(2),
    "Day" NUMBER(2),
    "Time" VARCHAR(10),
    "Amount" NUMBER(38,2),
    "UseChip" VARCHAR(100),
    "CardNumber" NUMBER(38,0),
    "MerchantCity" VARCHAR(100),
    "MerchantState" VARCHAR(100),
    "Zip" NUMBER(38,0),
    "MMC" NUMBER(38,0),
    "Errors" VARCHAR(1000),
    "IsFraud" VARCHAR(10)
);