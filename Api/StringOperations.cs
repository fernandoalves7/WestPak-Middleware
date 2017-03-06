using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Globalization;

namespace WestPakMiddleware.Api {
    public static class StringOperations {
        public static bool IsNumericOrNullOrWhiteSpace(string value) {
            return string.IsNullOrWhiteSpace(value) || IsNumeric(value);
        }

        public static bool IsNumeric(string value) {
            return !string.IsNullOrWhiteSpace(value) && value.ToCharArray().All(n => char.IsNumber(n));
        }

        public static System.Security.SecureString GetSecureString(string value) {
            var result = new System.Security.SecureString();

            foreach (char c in value.ToCharArray())
                result.AppendChar(c);

            return result;
        }

        public static int IndexOfAny(string text, params string[] array) {
            var result = new List<int>();

            foreach (var e in array) {
                var index = text.IndexOf(e);

                if (index != -1)
                    result.Add(index);
            }

            return result.Count != 0 ? result.Min() : -1;
        }

        public static bool StartsWithIgnoreCaseAny(string value1, params string[] array) {
            foreach (var p in array)
                if (StartsWithIgnoreCase(value1, p))
                    return true;

            return false;
        }

        public static bool StartsWithIgnoreCase(string value1, string value2) {
            if (value1 == null || value1.Length < value2.Length)
                return false;

            return value1.StartsWith(value2, StringComparison.CurrentCultureIgnoreCase);
        }

        public static string SubStr(string value, int length) {
            if (value == null || value.Length < length)
                return value;

            return value.Substring(0, length);
        }

        public static bool EqualsIgnoreCaseAny(string value1, params string[] array) {
            foreach (var p in array)
                if (EqualsIgnoreCase(value1, p))
                    return true;

            return false;
        }

        public static bool EqualsIgnoreCase(string value1, string value2) {
            return string.Equals(value1, value2, StringComparison.CurrentCultureIgnoreCase);
        }

        public static bool EqualsIgnoreCaseTrimRejectEmpties(string value1, string value2) {
            if (IsNullOrWhiteSpaceAny(value1, value2))
                return false;

            return string.Equals(value1.Trim(), value2.Trim(), StringComparison.CurrentCultureIgnoreCase);
        }

        public static string SerializeList(System.Collections.IList l, char needle) {
            var result = new StringBuilder();

            foreach (var item in l)
                result.Append(item).Append(needle);

            return result.ToString().TrimEnd(needle);
        }

        public static string SerializeDictionary<A, B>(Dictionary<A, B> dic, char pairNeedle, char enumNeedle) {
            var result = new StringBuilder();

            foreach (var item in dic)
                result.Append(item.Key.ToString()).Append(pairNeedle).Append(item.Value.ToString()).Append(enumNeedle);

            return result.ToString().TrimEnd(enumNeedle);
        }

        public static bool IsNullOrWhiteSpaceAll(params string[] array) {
            foreach (var p in array)
                if (!string.IsNullOrWhiteSpace(p))
                    return false;

            return true;
        }

        public static bool IsNullOrWhiteSpaceAny(params string[] array) {
            foreach (var p in array)
                if (string.IsNullOrWhiteSpace(p))
                    return true;

            return false;
        }

        public static string Trim(string value, params char[] elements) {
            if (value == null || value.Length == 0 || elements == null || elements.Length == 0)
                return value;

            foreach (var e in elements)
                value = value.Trim(e);

            return value;
        }

        public static string Capitalize(string value) {
            return CultureInfo.CurrentCulture.TextInfo.ToTitleCase(value.ToLower());
        }

        public static string RemoveAllWhitespaces(string value) {
            if (value == null)
                return value;

            return value.Replace("\r", "").Replace("\n", "");
        }

        public static string RemoveEmailAddressInvalidChars(string value) {
            if (value == null)
                return value;

            value = RemoveAllWhitespaces(value);

            var emailChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-.";
            var counter = 0;

            while (counter < value.Length) {
                if (!emailChars.Contains(value[counter])) {                 // There is an invalid email char
                    value = value.Replace(value[counter].ToString(), "");   // Remove all references of this char from the text
                    counter = 0;                                            // Go back to the beginning
                }

                counter++;
            }

            return value;
        }

        public static string GetFirstWord(string value) {
            if (value == null)
                return null;

            var parts = value.Split(' ');

            return parts[0];
        }

        public static string GetLastWord(string value) {
            if (value == null)
                return null;

            var parts = value.Split(' ');

            return parts[parts.Length - 1];
        }

        public static string GetSentenceExceptLastWord(string value) {
            if (value == null)
                return null;

            var parts = value.Split(' ');

            if (parts.Length == 1)
                return value;

            var lastWord = GetLastWord(value);

            return value.Substring(0, value.Length-lastWord.Length);
        }

        public static string ToString<T>(Nullable<T> value) where T : struct {
            if (value == null)
                return null;

            return Convert.ToString(value.Value);
        }
    }
}
