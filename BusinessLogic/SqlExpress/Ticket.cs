using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WestPakMiddleware.Api;

namespace WestPakMiddleware
{
    public sealed class Ticket
    {
        public Guid Id { get; set; }

        public int Version { get; set; }

        public string TicketType { get; set; }

        public string HarvestType { get; set; }

        public DateTime Date { get; set; }

        public string Number { get; set; }

        public int DriverId { get; set; }

        public int? HandlerId { get; set; }

        public int? RanchId { get; set; }

        public int? BuyerId { get; set; }

        public int? VarietyId { get; set; }

        public string ReferenceNumber { get; set; }

        public int? HarvesterId { get; set; }

        public int? VehicleId { get; set; }

        public decimal? GrossWeight { get; set; }

        public bool? Exported { get; set; }

        public float? Latitude { get; set; }

        public float? Longitude { get; set; }

        public DateTime? ModifiedDate { get; set; }

        public int Empties { get; set; }

        public int ActualPickUp { get; set; }

        public int ActualDrop { get; set; }

        public int TotalEmpties { get; set; }

        public DateTime? Timeout { get; set; }

        public string Comments { get; set; }

        public DateTime? PickDateTime { get; set; }

        public int? PickUp { get; set; }

        public bool DriverProcessed { get; set; }

        public bool BinDumpProcessed { get; set; }

        public int? DriverRecordId { get; set; }

        public int? GrowerRecordId { get; set; }

        public int? RanchRecordId { get; set; }

        public int? BuyerRecordId { get; set; }

        public string PurchaseOrderRecordId { get; set; }

        public long PickUpId { get; set; }

        public bool? EmptiesPickupAll { get; set; }

        public int? EmptiesPickupQuantity { get; set; }

        public bool? AmPickUp { get; set; }

        public string EnteredBy { get; set; }

        public string ShortHauledBy { get; set; }

        public string LongHauledBy { get; set; }

        public bool? FreskaStorage { get; set; }

        public bool? Piru { get; set; }

        public bool? MissedPickUp { get; set; }

        public string RootCause { get; set; }

        public string MissedPickUpComment { get; set; }

        public string TypeOfPick { get; set; }

        public int? CarryOver { get; set; }

        public string Status { get; set; }

        public bool DriverCreated { get; set; }

        public Driver Driver { get; set; }

        public Ranch Ranch { get; set; }

        public Vehicle Vehicle { get; set; }

        public Buyer Buyer { get; set; }

        public Variety Variety { get; set; }

        public List<TicketDetail> TicketDetails { get; set; }

        public static Ticket Parse(DataRow row)
        {
            var counter = 0;
            var result = new Ticket();

            result.Id = (Guid) row[counter++];
            result.Version = Convert.ToInt32(row[counter++]);
            result.TicketType = row[counter++] as string;
            result.HarvestType = row[counter++] as string;
            result.Number = row[counter++] as string;
            result.Date = (DateTime) row[counter++];
            result.DriverId = Convert.ToInt32(row[counter++]);
            result.HandlerId = row[counter++] as int?;
            result.RanchId = row[counter++] as int?;
            result.BuyerId = row[counter++] as int?;
            result.VarietyId = row[counter++] as int?;
            result.ReferenceNumber = row[counter++] as string;
            result.HarvesterId = row[counter++] as int?;
            result.VehicleId = row[counter++] as int?;
            result.GrossWeight = row[counter++] as decimal?;
            result.Exported = row[counter++] as bool?;
            result.Latitude = row[counter++] as float?;
            result.Longitude = row[counter++] as float?;

            counter++; //result.ModifiedDate = (DateTime) row[counter++];
            result.Empties = Convert.ToInt32(row[counter++]);
            result.ActualPickUp = Convert.ToInt32(row[counter++]);
            result.ActualDrop = Convert.ToInt32(row[counter++]);
            result.TotalEmpties = Convert.ToInt32(row[counter++]);
            result.Timeout = row[counter++] as DateTime?;
            result.Comments = row[counter++] as string;
            result.PickDateTime = row[counter++] as DateTime?;
            result.PickUp = row[counter++] as int?;
            result.DriverProcessed = Rms.ToBoolean(row[counter++] as string);
            result.BinDumpProcessed = Rms.ToBoolean(row[counter++] as string);
            result.DriverRecordId = row[counter++] as int?;
            result.GrowerRecordId = row[counter++] as int?;
            result.RanchRecordId = row[counter++] as int?;
            result.BuyerRecordId = row[counter++] as int?;

            result.Status = row[counter++] as string;
            result.DriverCreated = row[counter++] is DBNull ? false : Convert.ToBoolean(row[counter]);

            return result;           
        }

        public Grower Grower {
            get {
                return Ranch != null ? Ranch.Grower : null;
            }
        }

        public bool IsMoreRecentThan(DateTime? otherModifiedDate) {
            if (ModifiedDate == null && otherModifiedDate == null)
                return true;

            if (ModifiedDate == null && otherModifiedDate != null)
                return false;

            if (ModifiedDate != null && otherModifiedDate == null)
                return true;

            return ModifiedDate.Value > otherModifiedDate.Value;
        }

        public int CountTotalBins() {
            if (TicketDetails == null)
                return 0;

            return (from x in TicketDetails where x.BinId != null select x).Count();
        }

        public string GetGrowerName() {
            return Grower != null ? Grower.Name : null;
        }

        public string GetRanchName() {
            return Ranch != null ? Ranch.Name : null;
        }

        public string GetRanchAddress() {
            if (Ranch == null || Ranch.Address == null)
                return null;

            return Ranch.Address.Equals("N/A", StringComparison.CurrentCultureIgnoreCase) ? null : Ranch.Address;
        }

        public string GetDriverName() {
            return Driver != null ? Driver.Name : null;
        }

        public string GetDriverFirstName() {
            var name = GetDriverName();
            return name != null ? StringOperations.GetSentenceExceptLastWord(name) : null;
        }

        public string GetDriverLastName() {
            var name = GetDriverName();

            if (name.Split(' ').Length<2)
                return "";

            return name != null ? StringOperations.GetLastWord(name) : null;
        }

        public string GetBuyerName() {
            return Buyer != null ? Buyer.Name : null;
        }

        public string GetBuyerFirstName() {
            var name = GetBuyerName();
            return name != null ? StringOperations.GetSentenceExceptLastWord(name) : null;
        }

        public string GetBuyerLastName() {
            var name = GetBuyerName();
            return name != null ? StringOperations.GetLastWord(name) : null;
        }

        public string GetVehicleCodeOrName() {
            if (Vehicle != null) 
                return Vehicle.Code != null ? Vehicle.Code : Vehicle.Name;

            return null;
        }

        public string GetVarietyName() {
            return Variety != null ? Variety.Name : null;
        }

        public string GetMobileRecordId(string orgNumber) {
            return orgNumber + "-ticket-" + PurchaseOrderRecordId; // Candidate key at SQL Express side
        }

        public override string ToString()
        {
            var addressStr = GetRanchAddress();
            addressStr = addressStr != null ? " at " + addressStr : null;

            return Number + " (" + TicketType + " in " + Date.ToString("MM/dd/yyyy") + " for " + CountTotalBins() + " bins" + addressStr + ")";
        }
    }
}
