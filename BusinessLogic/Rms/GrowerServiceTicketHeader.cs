using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WestPakMiddleware.Api;

namespace WestPakMiddleware.BusinessLogic.Rms
{
    public sealed class GrowerServiceTicketHeader: WestPakMiddleware.Api.Rms.Node
    {
        public string mobileRecordId;

        public DateTime? CreationDate { get; set; }

        public long RecordId { get; set; }

        public string MobileRecordId {
            get {
                if (mobileRecordId != null)
                    return mobileRecordId;

                return TicketNumber;
            }

            set {
                mobileRecordId = value;
            }
        }

        public long RmsCodingTimestamp { get; set; }

        public long RmsTimestamp { get; set; }

        public string FunctionalGroupName { get; set; }

        public string FunctionalGroupObjectId { get; set; }

        public int Year { get; set; }

        public int Month { get; set; }

        public int Day { get; set; }

        public int Week { get; set; }

        public string Time { get; set; }

        public string OrganizationName { get; set; }

        public string OrganizationNumber { get; set; }

        public string TicketNumber { get; set; }

        public DateTime? DateTime { get; set; }

        public string ReferenceNumber { get; set; }

        public string DriverFirstName { get; set; }

        public string DriverLastName { get; set; }

        public long DriverRecordId { get; set; }

        public int TruckNumber { get; set; }

        public string Grower { get; set; }

        public long GrowerRecordId { get; set; }

        public string Ranch { get; set; }

        public long RanchRecordId { get; set; }

        public string Address { get; set; }

        public string BuyerFirstName { get; set; }

        public string BuyerLastName { get; set; }

        public long BuyerRecordId { get; set; }

        public string Variety { get; set; }

        public string TypeOfPick  { get; set; }

        public int? TotalBins { get; set; }

        public string Processed { get; set; }

        public int? PickUp { get; set; }

        public int? Empties { get; set; }

        public int? ActualPickUp { get; set; }

        public int? ActualDrop { get; set; }

        public int? TotalEmpties { get; set; }

        public string TimeOut { get; set; }

        public string Comments { get; set; }

        public float Latitude { get; set; }

        public float Longitude { get; set; }

        public DateTime? PickDateTime { get; set; }

        public string Status { get; set; }

        public bool DriverCreated { get; set; }

        public static GrowerServiceTicketHeader Parse(WestPakMiddleware.Api.Rms.Node r)
        {
            var result = new GrowerServiceTicketHeader();

            result.ObjectId = r.ObjectId;
            result.ObjectType = r.ObjectType;
            result.BarCode = r.BarCode;

            //result.CreationDate = r.GetCodingFieldAsDateTime("Creation Date");
            result.RecordId = r.GetCodingFieldAsInt32("RecordId");
            result.MobileRecordId = r.GetCodingField("MobileRecordId");
            result.RmsCodingTimestamp = r.GetCodingFieldAsLong("RMS Coding Timestamp");
            result.RmsTimestamp = r.GetCodingFieldAsLong("RMS Timestamp");
            result.FunctionalGroupName = r.GetCodingField("FunctionalGroupName");
            result.FunctionalGroupObjectId = r.GetCodingField("FunctionalGroupObjectId");
            result.Year = r.GetCodingFieldAsInt32("Year");
            result.Month = r.GetCodingFieldAsInt32("Month");
            result.Day = r.GetCodingFieldAsInt32("Day");
            result.Week = r.GetCodingFieldAsInt32("Week");
            result.OrganizationName = r.GetCodingField("OrganizationName");
            result.OrganizationNumber = r.GetCodingField("OrganizationNumber");
            result.TicketNumber = r.GetCodingField("Ticket Number");
            //result.DateTime = r.GetCodingFieldAsDateTime("DateTime");
            result.ReferenceNumber = r.GetCodingField("Reference Number");
            result.DriverFirstName = r.GetCodingField("Driver First Name");
            result.DriverLastName = r.GetCodingField("Driver Last Name");
            result.DriverRecordId = r.GetCodingFieldAsLong("DriverRecordId");
            result.TruckNumber = r.GetCodingFieldAsInt32("Truck Number");
            result.Grower = r.GetCodingField("Grower");
            result.GrowerRecordId = r.GetCodingFieldAsLong("GrowerRecordId");
            result.Ranch = r.GetCodingField("Ranch");
            result.RanchRecordId = r.GetCodingFieldAsLong("RanchRecordId");
            result.Address = r.GetCodingField("Address");
            result.BuyerFirstName = r.GetCodingField("Buyer First Name");
            result.BuyerLastName = r.GetCodingField("Buyer Last Name");
            result.BuyerRecordId = r.GetCodingFieldAsLong("BuyerRecordId");
            result.Variety = r.GetCodingField("Variety");
            result.TypeOfPick = r.GetCodingField("Type of Pick");
            result.TotalBins = r.GetCodingFieldAsNullableInt32("Total Bins");
            result.Processed = r.GetCodingField("Processed");
            result.PickUp = r.GetCodingFieldAsNullableInt32("Pick Up");
            result.Empties = r.GetCodingFieldAsNullableInt32("Empties");
            result.ActualPickUp = r.GetCodingFieldAsNullableInt32("Actual Pick Up");
            result.ActualDrop = r.GetCodingFieldAsNullableInt32("Actual Drop");
            result.TotalEmpties = r.GetCodingFieldAsNullableInt32("Total Empties");
            result.TimeOut = r.GetCodingField("Time Out");
            result.Comments = r.GetCodingField("Comments");
            result.Latitude = r.GetCodingFieldAsLong("Latitude");
            result.Longitude = r.GetCodingFieldAsLong("Longitude");
            //result.PickDateTime = r.GetCodingFieldAsDateTime("Pick DateTime");

            result.Status = r.GetCodingField("Status");
            result.DriverCreated = string.Equals(r.GetCodingField("Driver Created"), "TRUE", StringComparison.CurrentCultureIgnoreCase);

            return result;
        }

        public List<GrowerServiceTicketDetail> Childs { get; set; }

        public void AddChild(GrowerServiceTicketDetail d)
        {
            if (Childs == null)
                Childs = new List<GrowerServiceTicketDetail>();

            Childs.Add(d);
        }

        public bool ExistsChilds() {
            return Childs != null && Childs.Count > 0;
        }

        public bool ExistsTicketNumber() {
            return !string.IsNullOrWhiteSpace(TicketNumber);
        }

        public DateTime? GetRmsCodingTimestamp()
        {
            if (RmsCodingTimestamp == null)
                return null;

            return Cast.ToDateTime(RmsCodingTimestamp);
        }

        public string GetDriverName() {
            return (DriverFirstName + " " + DriverLastName).Trim();
        }

        public string GetBuyerName() {
            return (BuyerFirstName + " " + BuyerLastName).Trim();
        }

        public string GetTicketType() {
            return "RanchPickup";
        }

        public string GetHarvestType() {
            return TypeOfPick ?? "Regular";
        }

        public DateTime? GetDateTime() {
            return DateTime ?? System.DateTime.Now;
        }

        public long GetRmsCodingTimestampAsLong() {
            return RmsCodingTimestamp;
        }

        public override string ToString() {
            return TicketNumber + " (driver: " + DriverFirstName + " " + DriverLastName + ", grower: " + Grower + ")";
        }
    }
}

