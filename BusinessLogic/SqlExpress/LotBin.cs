using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware.BusinessLogic.SqlExpress {
    public class LotBin : WestPakMiddleware.Api.Rms.Node {
        public long RmsCodingTimestamp { get; set; }

        public Guid Id { get; set; }

        public decimal? Weight { get; set; }

        public Guid? LotId { get; set; }
        
        public Guid? TicketId { get; set; }

        public Guid? BinId { get; set; }

        public string LotNumber { get; set; }

        public string TicketNumber { get; set; }

        public string BinNumbers { get; set; }

        public DateTime? DateTime { get; set; }

        public string Processed { get; set; }
        
        public static LotBin Parse(WestPakMiddleware.Api.Rms.Node n, Dictionary<string, string> r) {
            var result = new LotBin();

            result.ObjectId = n.ObjectId;
            result.ObjectType = n.ObjectType;

            result.RmsCodingTimestamp = Convert.ToInt64(r["RMS Coding Timestamp"]);
            result.Weight = r["Weight"] == null ? null : (decimal?) Convert.ToDecimal(r["Weight"]);
            result.LotNumber = r["LotNumber"] as string;
            result.TicketNumber = r["Ticket Number"] as string;
            result.BinNumbers = r["BinNumbers"] as string;
            result.DateTime = WestPakMiddleware.Api.Rms.ToDateTime(r["DateTime"]); // TODO: Pass the hh:mm:ss
            result.Processed = r["Processed"] as string;

            return result;
        }

        public List<string> GetBinNumbers() {
            if (string.IsNullOrWhiteSpace(BinNumbers))
                return null;

            var result = new List<string>();
            var parts = BinNumbers.Split(',');

            foreach (var binNumber in parts)
                if (binNumber != null)
                    result.Add(binNumber.Trim());

            return result;
        }

        public override string ToString() {
            return Id.ToString() + " " + Weight;
        }
    }
}
