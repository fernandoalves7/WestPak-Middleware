using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware
{
    public class Bin
    {
        public Guid Id { get; set; }
        public int Version { get; set; }
        public string BinStatus { get; set; }
        public string Number { get; set; }
        public DateTime? Timestamp { get; set; }
        public bool? IsOrganic { get; set; }
        public int HandlerId { get; set; }
        public int? RanchId { get; set; }
        public decimal? TareWeight { get; set; }

        public static Bin Parse(DataRow row, int binStartIndex)
        {
            var counter = binStartIndex;
            var result = new Bin();

            result.Id = (Guid) row[counter++];
            result.Version = Convert.ToInt32(row[counter++]);
            result.BinStatus = row[counter++] as string;
            result.Number = row[counter++] as string;
            result.Timestamp = row[counter] is DBNull ? null : (DateTime?) row[counter++];
            result.IsOrganic = (bool?) row[counter++];
            result.HandlerId = Convert.ToInt32(row[counter++]);
            result.RanchId = row[counter++] as int?;
            result.TareWeight = row[counter++] as decimal?;

            return result;
        }
    }
}
