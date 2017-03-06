using System.Collections;
using System.Data;
using System;

namespace WestPakMiddleware
{
    /// <summary>
    /// This class gives you access to basic database functionality. 
    /// 
    /// This class should be herited by a more specific data provider class or be used in a 
    /// data processment and validation context.
    /// </summary>
    public class Database
    {
        protected DataSet ds;
        protected DataView dv;

        // State

        public static bool Exists(DataTable t, string field, int columnIndex)
        {
            foreach (DataRow r in t.Rows)
                if ((string)r[columnIndex] == field)
                    return true;

            return false;
        }
        public static bool ExistsNullField(DataTable t, int startIndex)
        {
            DataView dv;

            dv = new DataView(t);
            dv.RowStateFilter = DataViewRowState.CurrentRows;

            foreach (DataRowView r in dv)
                for (int i = startIndex; i < t.Columns.Count; i++)
                    if (r[i] is DBNull)
                        return true;

            return false;
        }
        public static bool ExistsNullField(DataTable t, int startIndex, int length)
        {
            DataView dv;

            dv = new DataView(t);
            dv.RowStateFilter = DataViewRowState.CurrentRows;

            foreach (DataRowView r in dv)
                for (int i = startIndex; i < startIndex + length; i++)
                    if (r[i] is DBNull)
                        return true;

            return false;
        }
        public static bool ExistsDuplicatedValuesString(DataTable t, int columnIndex)
        {
            for (int i = 0; i < t.Rows.Count; i++)
            {
                DataRow a = t.Rows[i];

                if (a.RowState != DataRowState.Deleted)
                {
                    for (int j = 0; j < t.Rows.Count; j++)
                    {
                        DataRow b = t.Rows[j];

                        if (b.RowState != DataRowState.Deleted)
                        {
                            string valueAtColumnA = (string)a[columnIndex];
                            string valueAtColumnB = (string)b[columnIndex];

                            if (i != j && valueAtColumnA.CompareTo(valueAtColumnB) == 0)
                                return true;
                        }
                    }
                }
            }

            return false;
        }

        // Read

        public static string GetTableName(string sqlString)
        {
            string tableName;

            tableName = sqlString.Substring(sqlString.ToUpper().IndexOf("FROM ") + 5);

            if (tableName.ToUpper().IndexOf(" ") >= 0)
                tableName = tableName.Substring(0, tableName.ToUpper().IndexOf(" "));

            return tableName;
        }
        public static string GetColumnName(DataTable t, int fieldIndex)
        {
            return t.Columns[fieldIndex].ColumnName;
        }
        public static ArrayList GetColumnsNames(DataRow r)
        {
            ArrayList columns = new ArrayList();

            foreach (DataColumn c in r.Table.Columns)
                columns.Add(c.ColumnName);

            return columns;
        }
        public static ArrayList GetRowFields(DataRow r)
        {
            ArrayList fields = new ArrayList();

            foreach (DataColumn c in r.Table.Columns)
                fields.Add(r[c]);

            return fields;
        }
        public static DataRow GetRow(DataTable t, string field, int columnIndex)
        {
            foreach (DataRow r in t.Rows)
                if (((string)r[columnIndex]) == field)
                    return r;

            return null;
        }
        public static DataRow GetRow(DataTable t, int field, int columnIndex)
        {
            foreach (DataRow r in t.Rows)
                if (r.RowState != DataRowState.Deleted)
                    if (Convert.ToInt32(r[columnIndex]) == field)
                        return r;

            return null;
        }
        public static DataRow GetRandomRow(DataTable t)
        {
            return t.Rows[(new Random()).Next(0, t.Rows.Count)];
        }
        public static ArrayList GetColumns(DataTable t)
        {
            ArrayList l = new ArrayList();

            foreach (DataColumn c in t.Columns)
                l.Add(c.ColumnName);

            return l;
        }
        public static object GetField(DataTable t, string comparationValue, int comparationIndex, int resultIndex)
        {
            foreach (DataRow r in t.Rows)
                if (((string)r[comparationIndex]) == comparationValue)
                    return r[resultIndex];

            return null;
        }
        public static object GetField(DataTable t, int comparationValue, int comparationIndex, int resultIndex)
        {
            foreach (DataRow r in t.Rows)
                if (Convert.ToInt32(r[comparationIndex]) == comparationValue)
                    return r[resultIndex];

            return null;
        }
        public static DataTable GetTable(DataTable t, string field, int comparationIndex)
        {
            DataTable t2 = new DataTable(t.TableName);

            foreach (DataColumn c in t.Columns)
                t2.Columns.Add(c.ColumnName, c.DataType, c.Expression);

            foreach (DataRow r in t.Rows)
                if (((string)r[comparationIndex]) == field)
                    t2.Rows.Add(r.ItemArray);

            return t2;
        }
        public static DataTable GetTable(DataTable t, int startIndex, int length)
        {
            DataTable t2 = new DataTable(t.TableName);
            int counter = 0;

            foreach (DataColumn c in t.Columns)
                t2.Columns.Add(c.ColumnName, c.DataType, c.Expression);

            foreach (DataRow r in t.Rows)
            {
                if (counter >= startIndex && counter < startIndex + length)
                    t2.Rows.Add(r.ItemArray);

                counter++;
            }

            return t2;
        }
        public static DataTable GetTable(DataTable t, int startIndex)
        {
            return GetTable(t, startIndex, t.Rows.Count - startIndex);
        }
        public static DataTable GetTable(DataSet ds, int index)
        {
            return ds.Tables[index];
        }
        public static DataTable GetTable(DataSet ds)
        {
            return ds.Tables[0];
        }
        public static DataTable GetTable(DataView dv)
        {
            DataTable t = new DataTable(dv.Table.TableName);

            CopyColumns(dv, t);
            CopyRows(dv, t);

            return t;
        }
        public static DataSet EncapsulateInDataSet(DataTable t)
        {
            DataSet ds = new DataSet();
            ds.Tables.Add(t.Copy());

            return ds;
        }
        public static object[] EncapsulateDataRow(DataRow r)
        {
            object[] columns = new object[r.Table.Columns.Count];

            for (int i = 0; i < r.Table.Columns.Count; i++)
                columns[i] = r[i];

            return columns;
        }
        public static void OverwriteTableFieldValue(DataTable t, int columnIndex, object columnValue)
        {
            foreach (DataRow r in t.Rows)
            {
                if (r.RowState != DataRowState.Deleted)
                    r[columnIndex] = columnValue;
            }
        }

        // Generic use

        protected static void CopyColumnsNames(DataTable t, ArrayList columns)
        {
            foreach (DataColumn c in t.Columns)
                columns.Add(c.ColumnName);
        }
        protected static void CopyColumns(DataView source, DataTable dest)
        {
            foreach (DataColumn c in source.Table.Columns)
                dest.Columns.Add(c.ColumnName, c.DataType, c.Expression);
        }
        protected static void CopyRows(DataView source, DataTable dest)
        {
            foreach (DataRowView r in source)
                dest.Rows.Add(r.Row.ItemArray);
        }
        protected static string BuildCondition(string fieldName, System.Type theType)
        {
            string condition = fieldName + "=";

            if (theType == typeof(string) || theType == typeof(DateTime))
                condition += "'{0}'";
            else
                condition += "{0}";

            return condition;
        }
        protected static void Debug(string theText)
        {
            System.Diagnostics.Debug.WriteLine(theText);
        }

        // Convertion

        public static int? ToInt32OrNull(object o) {
            return o is DBNull ? (int?)null : (int?)Convert.ToInt32(o);
        }
        public static bool? ToBoolOrNull(object o) {
            return o is DBNull ? (bool?)null : (bool?)Convert.ToBoolean(o);
        }
        public static string ToStringOrNull(object o) {
            return o is DBNull ? null : (string) o;
        }
    }
}