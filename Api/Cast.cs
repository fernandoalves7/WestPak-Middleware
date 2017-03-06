using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace WestPakMiddleware.Api {
    public static class Cast {
        public static bool ToBoolean(string value) {
            return value != null && (value.ToUpper() == "TRUE" || value.ToUpper() == "YES" || value == "1");
        }

        public static double ToDoubleOrZero(string value) {
            try {
                return Convert.ToDouble(value);
            } catch {
                return 0;
            }
        }

        public static int ToInt32OrDefault(string value, int defaultValue) {
            try {
                return Convert.ToInt32(value);
            } catch {
                return defaultValue;
            }
        }

        public static int ToInt32OrMinusOne(string value) {
            try {
                return Convert.ToInt32(value);
            } catch {
                return -1;
            }
        }

        public static int ToInt32OrZero(string value) {
            try {
                return Convert.ToInt32(value);
            } catch {
                return 0;
            }
        }

        public static DateTime? ToDateTimeOrNull(string date, string formatString) {
            if (string.IsNullOrWhiteSpace(date))
                return null;

            return DateTime.ParseExact(date, formatString, CultureInfo.InvariantCulture);
        }

        public static DateTime ToDateTime(string date, string formatString) {
            return DateTime.ParseExact(date, formatString, CultureInfo.InvariantCulture);
        }

        public static DateTime? ToDateTime(long unixTimestamp) {
            return new DateTime(1970, 1, 1, 0, 0, 0).AddSeconds(unixTimestamp / 1000).ToLocalTime();
        }

        public static DateTime? ToDateTime(string unixTimestamp) {
            if (string.IsNullOrWhiteSpace(unixTimestamp))
                return null;

            return ToDateTime(Convert.ToInt64(unixTimestamp));
        }

        public static string ToUnixTimestamp(DateTime value) {
            var span = (value - new DateTime(1970, 1, 1, 0, 0, 0, 0).ToLocalTime());
            return Convert.ToString(Convert.ToUInt64((double) span.TotalSeconds) * 1000);
        }

        public static System.Security.SecureString ToSecureString(string value) {
            var result = new System.Security.SecureString();

            foreach (var c in value)
                result.AppendChar(c);

            return result;
        }
    }
}
