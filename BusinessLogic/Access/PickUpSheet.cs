using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware.BusinessLogic.Access {
    public sealed class PickUpSheet {
        public long Id { get; set; }
        public DateTime Date { get; set; }
        public string GroveName { get; set; }
        public string Driver { get; set; }
        public int? PickUp { get; set; }
        public int? CarryOver { get; set; }
        public int? Empties { get; set; }
        public int? ActualPickedUp { get; set; }
        public int? ActualEmptiesAtGrove { get; set; }
        public string Comments { get; set; }
        public bool? AmPickup { get; set; }
        public int? ShortHauledBy { get; set; }
        public int? LongHauledBy { get; set; }
        public bool? FreskaStorage { get; set; }
        public string Variety { get; set; }
        public int? EnteredBy { get; set; }
        public string OtherDuties { get; set; }
        public bool? MissedPickUp { get; set; }
        public string RootCause { get; set; }
        public bool? Piru { get; set; }
        public string MissedPickUpComment { get; set; }
        public string TypeOfPick { get; set; }
        public string PickUpSheetNumber { get; set; }
        public string NorthDutyOfficer { get; set; }
        public string SouthDutyOfficer { get; set; }
        public string Truck { get; set; }
        public int? WeekNumber { get; set; }
        public DateTime? ModifiedDate { get; set; }
        public bool? EmptiesPickUpAll { get; set; }
        public int? EmptiesPickUpQty { get; set; }

        public string Area { get; set; }
        public string Buyer { get; set; }

        public static PickUpSheet Parse(DataRow row) {
            var result = new PickUpSheet();
            var counter = 0;

            result.Id = Convert.ToInt32(row[counter++]);
            result.Date = (DateTime)row[counter++];
            counter++; //result.GroveName = row[counter++] as string;
            result.Driver = row[counter++] as string;
            result.PickUp = Database.ToInt32OrNull(row[counter++]);
            result.CarryOver = Database.ToInt32OrNull(row[counter++]);
            result.Empties = Database.ToInt32OrNull(row[counter++]);
            result.ActualPickedUp = Database.ToInt32OrNull(row[counter++]);
            result.ActualEmptiesAtGrove = Database.ToInt32OrNull(row[counter++]);
            result.Comments = row[counter++] as string;
            result.AmPickup = Database.ToBoolOrNull(row[counter++]);
            result.ShortHauledBy = Database.ToInt32OrNull(row[counter++]);
            result.LongHauledBy = Database.ToInt32OrNull(row[counter++]);
            result.FreskaStorage = Database.ToBoolOrNull(row[counter++]);
            result.Variety = row[counter++] as string;
            result.EnteredBy = Database.ToInt32OrNull(row[counter++]);
            result.OtherDuties = row[counter++] as string;
            result.MissedPickUp = Database.ToBoolOrNull(row[counter++]);
            result.RootCause = row[counter++] as string;
            result.Piru = Database.ToBoolOrNull(row[counter++]);
            result.MissedPickUpComment = row[counter++] as string;
            result.TypeOfPick = row[counter++] as string;
            result.PickUpSheetNumber = row[counter++] as string;
            result.NorthDutyOfficer = row[counter++] as string;
            result.SouthDutyOfficer = row[counter++] as string;
            result.Truck = row[counter++] as string;
            result.WeekNumber = Database.ToInt32OrNull(row[counter++]);
            result.ModifiedDate = (DateTime)row[counter++];
            result.EmptiesPickUpAll = Database.ToBoolOrNull(row[counter++]);
            result.EmptiesPickUpQty = Database.ToInt32OrNull(row[counter++]);

            result.GroveName = row[counter++] as string;

            return result;
        }

        public string GetMobileRecordId(string orgNumber) {
            return orgNumber + "-pickupsheet-" + Id;
        }

        public override string ToString() {
            return Id + " (Date:" + Date + ", grove:" + GroveName + ", driver:" + Driver + ", truck:" + Truck + ", modified:" + ModifiedDate + ")";
        }
    }
}
