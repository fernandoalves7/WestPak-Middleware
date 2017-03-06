using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware
{
    public class TicketDetail
    {
        public Guid Id { get; set; }
        public string BinLevel { get; set; }
        public Guid BinId { get; set; }
        public Guid TicketId { get; set; }
        public DateTime? ModifiedDate { get; set; }

        public string Status { get; set; }
        public decimal? Weight { get; set; }

        public Bin Bin { get; set; }

        public string GetMobileRecordId(string orgNumber, string ticketNumber) {
            return orgNumber + "-ticket-" + ticketNumber + "-" + Id;
        }

        public string GetBinNumber() {
            return Bin != null ? Bin.Number : null;
        }

        public double GetBinLevelQuantity() {
            if (BinLevel == null)
                return 0;

            switch (BinLevel.ToLower()) {
                case "full": return 1;
                case "half": return 0.5;
                case "empty": return 0;
            }

            return 0;
        }

        public static TicketDetail Parse(DataRow row)
        {
            var result = new TicketDetail();

            result.Id = (Guid) row[0];
            result.BinLevel = row[1] as string;
            result.BinId = (Guid) row[2];

            if (!(row[3] is DBNull))
                result.TicketId = (Guid) row[3];

            if (result.BinId != null)
                result.Bin = Bin.Parse(row, 5);

            return result;
        }

        public override string ToString()
        {
            return BinId + ", " + BinLevel;
        }
    }
}
