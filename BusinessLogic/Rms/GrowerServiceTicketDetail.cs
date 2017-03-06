using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware.BusinessLogic.Rms
{
    public sealed class GrowerServiceTicketDetail: WestPakMiddleware.Api.Rms.Node
    {
        public string CreationDate { get; set; }
        public string RecordId { get; set; }
        public string MobileRecordId { get; set; }
        public long RMSCodingTimestamp { get; set; }
        public long RMSTimestamp { get; set; }
        public string FunctionalGroupName { get; set; }
        public string FunctionalGroupObjectId { get; set; }
        public string MasterBarcode { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public int Week { get; set; }
        public string Time { get; set; }
        public string OrganizationName { get; set; }
        public long OrganizationNumber { get; set; }
        public string TicketNumber { get; set; }
        public string ManufacturerSerialNumber { get; set; }
        public float Quantity { get; set; }
        public DateTime? DateTime { get; set; }
        public string BinNumber { get; set; }

        public string Status { get; set; }
        public decimal? Weight { get; set; }

        public static GrowerServiceTicketDetail Parse(Api.Rms.Node n, Dictionary<string, string> r)
        {
            var result = new GrowerServiceTicketDetail();

            result.ObjectId = n.ObjectId;
            result.ObjectType = n.ObjectType;

            //result = r.GetCodingField("Creation Date");
            result.RecordId = r["RecordId"];
            result.MobileRecordId = r["MobileRecordId"];
            result.RMSCodingTimestamp = Convert.ToInt64(r["RMS Coding Timestamp"]);
            result.RMSTimestamp = Convert.ToInt64(r["RMS Timestamp"]);
            result.FunctionalGroupName = r["FunctionalGroupName"];
            result.FunctionalGroupObjectId = r["FunctionalGroupObjectId"];
            result.MasterBarcode = r["Master Barcode"];
            result.Year = Convert.ToInt32(r["Year"]);
            result.Month = Convert.ToInt32(r["Month"]);
            result.Day = Convert.ToInt32(r["Day"]);
            result.Week = Convert.ToInt32(r["Week"]);
            result.Time = r["Time"];
            result.OrganizationName = r["Organization Name"];
            result.OrganizationNumber = Convert.ToInt32(r["Organization Number"]);
            result.TicketNumber = r["Ticket Number"];
            result.ManufacturerSerialNumber = r["Manufacturer Serial Number"];
            result.Quantity = float.Parse(r["Quantity"], NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.GetCultureInfo("en-US"));
            //result.DateTime = r["DateTime"];
            result.BinNumber = r["Bin Number"];

            result.Status = r["Status"];
            result.Weight = r["Weight"] == null ? null : (decimal?) Convert.ToDecimal(r["Weight"]);

            return result;
        }

        public static GrowerServiceTicketDetail Parse(WestPakMiddleware.Api.Rms.Node r)
        {
            var result = new GrowerServiceTicketDetail();

            result.ObjectId = r.ObjectId;
            result.ObjectType = r.ObjectType;

            //result = r.GetCodingField("Creation Date");
            result.RecordId = r.GetCodingField("RecordId");
            result.MobileRecordId = r.GetCodingField("MobileRecordId");
            result.RMSCodingTimestamp = r.GetCodingFieldAsLong("RMS Coding Timestamp");
            result.RMSTimestamp = r.GetCodingFieldAsLong("RMS Timestamp");
            result.FunctionalGroupName = r.GetCodingField("FunctionalGroupName");
            result.FunctionalGroupObjectId = r.GetCodingField("FunctionalGroupObjectId");
            result.MasterBarcode = r.GetCodingField("Master Barcode");
            result.Year = r.GetCodingFieldAsInt32("Year");
            result.Month = r.GetCodingFieldAsInt32("Month");
            result.Day = r.GetCodingFieldAsInt32("Day");
            result.Week = r.GetCodingFieldAsInt32("Week");
            result.Time = r.GetCodingField("Time");
            result.OrganizationName = r.GetCodingField("Organization Name");
            result.OrganizationNumber = r.GetCodingFieldAsInt32("Organization Number");
            result.TicketNumber = r.GetCodingField("Ticket Number");
            result.ManufacturerSerialNumber = r.GetCodingField("Manufacturer Serial Number");
            result.Quantity = r.GetCodingFieldAsInt32("Quantity");
            //result.DateTime = r.GetCodingField("DateTime");
            result.BinNumber = r.GetCodingField("Bin Number");

            result.Status = r.GetCodingField("Status");
            result.Weight = r.GetCodingFieldAsNullableDecimal("Weight");

            return result;
        }

        public string GetBinLevel() {
            if (Quantity >= 1)
                return "Full";
            else if (Quantity >= 0.75 && Quantity < 1)
                return "ThreeQuarter";
            else if (Quantity >= 0.5 && Quantity < 0.75)
                return "Half";
            else if (Quantity >= 0.25 && Quantity < 0.5)
                return "Quarter";

            return "Empty";
        }
    }
}
