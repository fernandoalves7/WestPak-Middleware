/********************************************************************************
 * Update Access database with sync capabilities with other systems living 
 * in the WestPak ecosystem.
 * 
 * History
 *******************************************************************************
 * 01/24/2017 - Fernando ML Alves
 * 
 *******************************************************************************
 */

-- DDL

ALTER TABLE [Pickup Sheet Detail] ADD ModifiedDate TIMESTAMP;
ALTER TABLE [Pickup Sheet Detail] ADD [Empties pick up] INT;


-- DML

UPDATE [Pickup Sheet Detail] SET ModifiedDate=NOW();


-- Temp

SELECT * FROM [Pickup Sheet Detail] WHERE ModifiedDate>#2017-01-20 12:31:45#