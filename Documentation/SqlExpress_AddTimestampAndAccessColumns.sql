/********************************************************************************
 * Updates SQL Express to reflect sync capabilities with other systems living 
 * in the WestPak ecosystem.
 * 
 * History
 *******************************************************************************
 * 01/08/2017 - Fernando ML Alves
 * Add to the Ticket table a modified date timestamp field and additional 
 * fields reflecting the access database (through the Pickup / Drop off forms 
 * vba app).
 * 
 *******************************************************************************
 */
USE BinTrack;
GO

BEGIN TRY 
    BEGIN TRANSACTION 
		SELECT TOP 10 * FROM Ticket;

		-- Ticket

		ALTER TABLE Ticket ADD ModifiedDate TIMESTAMP	;
		ALTER TABLE Ticket ADD Empties INT;
		ALTER TABLE Ticket ADD ActualPickUp INT;
		ALTER TABLE Ticket ADD ActualDrop INT;
		ALTER TABLE Ticket ADD TotalEmpties INT;
		ALTER TABLE Ticket ADD Timeout DATETIME;
		ALTER TABLE Ticket ADD Comments TEXT;
		ALTER TABLE Ticket ADD PickDateTime DATETIME;
		ALTER TABLE Ticket ADD PickUp INT;
		ALTER TABLE Ticket ADD DriverProcessed BIT;
		ALTER TABLE Ticket ADD BinDumpProcessed BIT;
		ALTER TABLE Ticket ADD DriverRecordId INT;
		ALTER TABLE Ticket ADD GrowerRecordId INT;
		ALTER TABLE Ticket ADD RanchRecordId INT;
		ALTER TABLE Ticket ADD BuyerRecordId INT;

		-- TicketDetail

		ALTER TABLE TicketDetail ADD ModifiedDate TIMESTAMP;

		SELECT TOP 10 * FROM Ticket;
    COMMIT TRANSACTION
END TRY
BEGIN CATCH
    ROLLBACK TRANSACTION
    RAISERROR (N'Error while altering table(s)', 1, 1);
END CATCH;
GO