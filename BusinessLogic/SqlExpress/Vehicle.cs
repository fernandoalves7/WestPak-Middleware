using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware
{
    public class Vehicle
    {
        public int Id { get; set; }
        public string Code { get; set; }
        public string Name { get; set; }
        public string MaximumBins { get; set; }
        public bool? Inactive { get; set; }

        public static Vehicle Parse(DataRow row, int columnStartIndex) {
            var result = new Vehicle();
            var counter = columnStartIndex;

            var id = row[counter++] as int?;

            if (id == null)
                return null;

            result.Id = id.Value;
            result.Code = row[counter++] as string;
            result.Name = row[counter++] as string;
            result.MaximumBins = row[counter++] as string;
            result.Inactive = row[counter++] as bool?;

            return result;
        }
    }
}
