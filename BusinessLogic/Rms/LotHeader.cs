using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware.BusinessLogic.Rms {
    public class LotHeader : WestPakMiddleware.Api.Rms.Node {
        public string mobileRecordId;

        public DateTime? CreationDate { get; set; }

        public long RmsCodingTimestamp { get; set; }

        public long RmsTimestamp { get; set; }

        public long RecordId { get; set; }

        public string MobileRecordId {
            get {
                if (mobileRecordId != null)
                    return mobileRecordId;

                return LotNumber;
            }

            set {
                mobileRecordId = value;
            }
        }

        public string FunctionalGroupName { get; set; }

        public string FunctionalGroupObjectId { get; set; }

        public int Year { get; set; }

        public int Month { get; set; }

        public int Day { get; set; }

        public string OrganizationName { get; set; }

        public string OrganizationNumber { get; set; }

        public int SqlExpressId { get; set; }

        public string LotNumber { get; set; }

        public DateTime? ScheduleDateTime { get; set; }

        public string DumpStatus { get; set; }

        public int? VarietyId { get; set; }

        public string VarietyName { get; set; }

        public int? CommodityId { get; set; }

        public string CommodityName { get; set; }

        public DateTime? StartDumpingDateTime { get; set; }

        public DateTime? FinishedDumpingDateTime { get; set; }

        public int? FacilityId { get; set; }

        public string Processed { get; set; }
    }
}
