using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware
{
    public class Ranch
    {
        public int Id { get; set; }
        public string Code { get; set; }
        public string Name { get; set; }
        public string Address { get; set; }
        public string City { get; set; }
        public string Notes { get; set; }
        public bool? Inactive { get; set; }
        public int? GrowerId { get; set; }
        public int? VarietyId { get; set; }
        public string FamousRanch { get; set; }

        public int? RecordId { get; set; }

        public static Ranch Parse(DataRow row, int ranchColumnStartIndex)
        {
            var result = new Ranch();
            var counter = ranchColumnStartIndex;

            result.Id = Convert.ToInt32(row[counter++]);
            result.Code = row[counter++] as string;
            result.Name = row[counter++] as string;
            result.Address = row[counter++] as string;
            result.City = row[counter++] as string;
            result.Notes = row[counter++] as string;
            result.Inactive = row[counter++] as bool?;
            result.GrowerId = row[counter++] as int?;
            result.VarietyId = row[counter++] as int?;
            result.FamousRanch = row[counter++] as string;

            if (row.ItemArray.Length < counter) {
                var grower = new Grower();

                grower.Id = Convert.ToInt32(row[counter++]);
                grower.Code = row[counter++] as string;
                grower.Name = row[counter++] as string;
                grower.Inactive = row[counter++] as bool?;
                grower.DefaultBuyerId = row[counter++] as int?;

                result.Grower = grower;
            }

            return result;
        }

        public static Ranch ParseNode(WestPakMiddleware.Api.Rms.Node r) {
            var result = new Ranch();

            result.RecordId = r.GetCodingFieldAsNullableInt32("RecordId");
            result.Name = r.GetCodingField("Store Name");
            //result.FamousRanch = r.GetCodingField("FamousRanch");

            return result;
        }

        public static Ranch ParseRow(DataRow row) {
            return Parse(row, 0);
        }

        public override string ToString() {
            return Id + ", " + Code + ", " + Name + "";
        }

        public Grower Grower { get; set; }
        public Variety Variety { get; set; }
    }
}
