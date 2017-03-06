use BinTrack

select d.Name as Driver, r.Name as Ranch, v.Name as Variety, t.* from Ticket t, Driver d, Ranch r, Variety v where t.DriverId=d.Id and t.RanchId=r.Id and t.VarietyId=v.Id and t.Number='1705013'
select d.BinLevel, d.BinId, d.Status, d.Weight, d.ModifiedDate from Ticket t, TicketDetail d where t.Id=d.TicketId and t.Number='1705013'

select top 10 * from Ticket order by ModifiedDate desc

delete from TicketDetail where TicketId=(select id from Ticket where Number='666')
delete from Ticket where Number='666'

delete from TicketDetail where TicketId=(select id from Ticket where Number='1799002')
delete from Ticket where Number='1799002'

-----------

use BinTrack

select top 10 * from lot where DumpStatus='Not Started'

declare @TicketNumber varchar(255) = '1705018';
select * from ticket where number=@TicketNumber;
select d.*, b.Number as BinNumber from ticket t, TicketDetail d, Bin b where t.number=@TicketNumber and t.Id=d.TicketId and d.BinId=b.Id;

declare @LN varchar(255) = 'L17T100002';
select top 10 * from lot where LotNumber=@LN
select top 10 lb.* from LotBin lb, lot l where l.LotNumber=@LN and l.Id=lb.LotId


SELECT * FROM Ticket where id='9AB41314-D895-4C8D-ABE5-64C3F352EB92'
