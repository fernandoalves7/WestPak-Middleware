using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WestPakMiddleware.BusinessLogic;

namespace WestPakMiddleware
{
    public class Driver
    {
        public int Id { get; set; }
        public string Code { get; set; }
        public string Name { get; set; }
        public bool? Inactive { get; set; }
        public int? DefaultVehicleId { get; set; }
        public int? ContractorId { get; set; }
        public string DriverType { get; set; }

        public int? RecordId { get; set; }

        public static Driver ParseNode(WestPakMiddleware.Api.Rms.Node r) {
            var result = new Driver();

            result.RecordId = r.GetCodingFieldAsInt32("RecordId");
            result.Name = r.GetCodingField("First Name") + " " + r.GetCodingField("Last Name");

            return result;
        }

        public static Driver ParseRow(DataRow row) {
            return Parse(row, 0);
        }

        public static Driver Parse(DataRow row, int columnStartIndex) {
            var result = new Driver();
            var counter = columnStartIndex;

            result.Id = Convert.ToInt32(row[counter++]);
            result.Code = row[counter++] as string;
            result.Name = row[counter++] as string;
            result.Inactive = row[counter++] as bool?;
            result.DefaultVehicleId = row[counter++] as int?;
            result.ContractorId = row[counter++] as int?;
            result.DriverType = row[counter++] as string;

            return result;
        }

        public override string ToString() {
            return Id + ", " + Name + "";
        }
    }
}
