using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WestPakMiddleware.BusinessLogic;

namespace WestPakMiddleware
{
    public class Grower
    {
        public int Id { get; set; }
        public string Code { get; set; }
        public string Name { get; set; }
        public bool? Inactive { get; set; }
        public int? DefaultBuyerId { get; set; }

        public int? RecordId { get; set; }
        public string Company { get; set; }

        public static Grower ParseNode(WestPakMiddleware.Api.Rms.Node r) {
            var result = new Grower();

            result.RecordId = r.GetCodingFieldAsNullableInt32("RecordId");
            result.Name = r.GetCodingField("First Name") + " " + r.GetCodingField("Last Name");
            result.Company = r.GetCodingField("Company");

            return result;
        }

        public static Grower ParseRow(DataRow row) {
            return Parse(row, 0);
        }

        public static Grower Parse(DataRow row, int columnStartIndex) {
            var result = new Grower();
            var counter = columnStartIndex;

            result.Id = Convert.ToInt32(row[counter++]);
            result.Code = row[counter++] as string;
            result.Name = row[counter++] as string;
            result.Inactive = row[counter++] as bool?;
            result.DefaultBuyerId = row[counter++] as int?;

            return result;
        }

        public override string ToString() {
            return Id + ", " + Code + ", " + Name + "";
        }
    }
}
