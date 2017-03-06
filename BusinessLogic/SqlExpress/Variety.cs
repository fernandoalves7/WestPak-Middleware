using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware
{
    public class Variety
    {
        public int Id { get; set; }
        public string Code { get; set; }
        public string Name { get; set; }
        public bool? Organic { get; set; }
        public bool? Inactive { get; set; }
        public int? ExternalId { get; set; }
        public int? CommodityId { get; set; }

        public int? RecordId { get; set; }
        public string ItemNumber { get; set; }

        public static Variety Parse(DataRow row, int columnStartIndex) {
            var result = new Variety();
            var counter = columnStartIndex;

            result.Id = Convert.ToInt32(row[counter++]);
            result.Code = row[counter++] as string;
            result.Name = row[counter++] as string;
            result.Organic = row[counter++] as bool?;
            result.Inactive = row[counter++] as bool?;
            result.ExternalId = row[counter++] as int?;
            result.CommodityId = row[counter++] as int?;

            return result;
        }

        public static Variety ParseNode(WestPakMiddleware.Api.Rms.Node r) {
            var result = new Variety();

            result.RecordId = r.GetCodingFieldAsNullableInt32("RecordId");
            result.Name = r.GetCodingField("Name");
            result.ItemNumber = r.GetCodingField("Number");

            return result;
        }

        public static Variety ParseRow(DataRow row) {
            return Parse(row, 0);
        }

        public override string ToString() {
            return Id + ", " + Code + ", " + Name + "";
        }
    }
}
