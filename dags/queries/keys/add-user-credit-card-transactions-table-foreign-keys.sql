ALTER TABLE TEST.APP."UserCreditCardTransactions" 
ADD FOREIGN KEY ("CardNumber") REFERENCES TEST.APP."Cards"("CardNumber");