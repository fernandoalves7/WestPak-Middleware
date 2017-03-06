using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware.BusinessLogic.SqlExpress {
    public class Lot : WestPakMiddleware.Api.Rms.Node {
        public long RmsCodingTimestamp { get; set; }

        public Guid Id { get; set; }

        public string LotNumber { get; set; }

        public DateTime? ScheduleDate { get; set; }

        public string DumpStatus { get; set; }

        public string PackStatus { get; set; }

        public int? LineId { get; set; }

        public int? VarietyId { get; set; }

        public int? CommodityId { get; set; }

        public DateTime? StartedDumpingDateTime { get; set; }

        public DateTime? FinishedDumpingDateTime { get; set; }

        public DateTime? StartedPackingDateTime { get; set; }

        public DateTime? FinishedPackingDateTime { get; set; }

        public string VarietyName { get; set; }

        public string CommodityName { get; set; }

        public void SetVarietyAndCommodityNames(string varietyName, string commodityName) {
            VarietyName = varietyName;
            CommodityName = commodityName;
        }

        public static Lot Parse(DataRow row) {
            var result = new Lot();
            var counter = 0;

            result.Id = (Guid)row[counter++];
            result.LotNumber = row[counter++] as string;
            result.ScheduleDate = row[counter] is DBNull ? null : (DateTime?)row[counter++];
            result.DumpStatus = row[counter++] as string;
            result.PackStatus = row[counter++] as string;
            result.LineId = row[counter++] as int?;
            result.VarietyId = row[counter++] as int?;
            result.CommodityId = row[counter++] as int?;
            result.StartedDumpingDateTime = row[counter] is DBNull ? null : (DateTime?)row[counter++];
            result.FinishedDumpingDateTime = row[counter] is DBNull ? null : (DateTime?)row[counter++];
            result.StartedPackingDateTime = row[counter] is DBNull ? null : (DateTime?)row[counter++];
            result.FinishedDumpingDateTime = row[counter] is DBNull ? null : (DateTime?)row[counter++];

            return result;
        }

        public static Lot Parse(WestPakMiddleware.Api.Rms.Node r) {
            var result = new Lot();

            result.ObjectId = r.ObjectId;
            result.ObjectType = r.ObjectType;
            result.BarCode = r.BarCode;

            if (!string.IsNullOrWhiteSpace(r.GetCodingField("SqlExpressId"))) {
                var lotGuid = WestPakMiddleware.Api.Rms.ToNullableGuid(r.GetCodingField("SqlExpressId"));

                if (lotGuid != null)
                    result.Id = lotGuid.Value;
            }

            result.RmsCodingTimestamp = r.GetCodingFieldAsLong("RMS Coding Timestamp");
            result.LotNumber = r.GetCodingField("LotNumber");
            result.ScheduleDate = WestPakMiddleware.Api.Rms.FromMilitaryDateTime(r.GetCodingField("ScheduleDateTime"));
            result.DumpStatus = r.GetCodingField("DumpStatus");
            result.VarietyId = r.GetCodingFieldAsNullableInt32("VarietyId");
            result.VarietyName = r.GetCodingField("VarietyName");
            result.CommodityId = r.GetCodingFieldAsNullableInt32("CommodityId");
            result.CommodityName = r.GetCodingField("CommodityName");
            result.StartedDumpingDateTime = WestPakMiddleware.Api.Rms.FromMilitaryDateTime(r.GetCodingField("StartDumpingDateTime"));
            result.FinishedDumpingDateTime = WestPakMiddleware.Api.Rms.FromMilitaryDateTime(r.GetCodingField("FinishedDumpingDateTime"));

            return result;
        }

        public string GetMobileRecordId(string orgNumber) {
            return orgNumber + "-lot-" + LotNumber;
        }

        public override string ToString() {
            return LotNumber + ", Schedule: " + ScheduleDate + ", Dump: " + DumpStatus + ", Pack: " + PackStatus;
        }

        public string GetFacilityId() {
            return LotNumber.Length >= 4 ? LotNumber.Substring(3, 1) : null;
        }

        public bool ExistsLotBins() {
            return LotBins != null && LotBins.Count > 0;
        }

        public List<LotBin> LotBins { get; set; }

        public void AddLotBins(LotBin lotBin) {
            if (LotBins == null)
                LotBins = new List<LotBin>();

            LotBins.Add(lotBin);
        }
    }
}
