using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware
{
    public class Buyer
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public string Code { get; set; }
        public bool? OutsideBuyer { get; set; }
        public bool? Inactive { get; set; }

        public int? RecordId { get; set; }

        public static Buyer Parse(DataRow row, int columnStartIndex) {
            var result = new Buyer();
            var counter = columnStartIndex;

            result.Id = Convert.ToInt32(row[counter++]);
            result.Code = row[counter++] as string;
            result.Name = row[counter++] as string;
            result.OutsideBuyer = row[counter++] as bool?;
            result.Inactive = row[counter++] as bool?;

            return result;
        }

        public static Buyer ParseNode(WestPakMiddleware.Api.Rms.Node r) {
            var result = new Buyer();

            result.RecordId = r.GetCodingFieldAsNullableInt32("RecordId");
            result.Name = r.GetCodingField("First Name") + " " + r.GetCodingField("Last Name");

            return result;
        }

        public static Buyer ParseRow(DataRow row) {
            return Parse(row, 0);
        }

        public override string ToString() {
            return Id + ", " + Code + ", " + Name + "";
        }
    }
}
