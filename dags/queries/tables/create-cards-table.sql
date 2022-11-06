CREATE OR REPLACE TABLE TEST.APP."Cards" (
    "User" NUMBER(38,0) NOT NULL,
    "CardIndex" VARCHAR(100),
    "CardBrand" VARCHAR(100),
    "CardType" VARCHAR(100),
    "CardNumber" NUMBER(38,0) NOT NULL,
    "Expires" VARCHAR(10),
    "CVV" NUMBER(3),
    "HasChip" VARCHAR(10),
    "CardsIssued" NUMBER(38,0),
    "CreditLimit" NUMBER(38,2),
    "AcctOpenDate" VARCHAR(10),
    "YearPinLastChanged" NUMBER(38,0),
    "CardOnDarkWeb" VARCHAR(10)
);