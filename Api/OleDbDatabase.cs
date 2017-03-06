using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware.Api {
    public sealed class OleDbDatabase : Database {
        private DataSet ds;
        private DataView dv;
        private OleDbConnection cn;
        private OleDbCommand cm;
        private OleDbDataReader dr;
        private OleDbDataAdapter da;
        private OleDbTransaction tr;
        private OleDbCommandBuilder cb;

        private string filepath;

        public OleDbDatabase(string filepath) : base() {
            this.filepath = filepath;
        }
        public bool IsConnected() {
            if (cn == null)
                return false;
            else
                if (cn.State != ConnectionState.Open)
                    return false;
                else
                    return true;
        }
        public void Connect() {
            cn = new OleDbConnection(@"Provider=Microsoft.ACE.O‌​LEDB.12.0;Data Source=" + filepath + ";");
            cn.Open();
        }
        public void Disconnect() {
            if (cn != null || cn.State != ConnectionState.Closed) {
                cn.Close();
                cn = null;
            }
        }

        // Select

        public bool Exists(string sqlString) {
            bool result;

            Connect();
            cm = new OleDbCommand(sqlString, cn);
            dr = cm.ExecuteReader();
            result = dr.HasRows;
            dr.Close();
            Disconnect();

            return result;
        }
        public bool ExistsIndex(string tableName, string indexName) {
            return Exists("SHOW INDEX FROM " + tableName + " WHERE key_name='" + indexName + "';");
        }
        public long Count(string tableName, string condition) {
            Connect();
            string sqlString = "SELECT COUNT(*) FROM " + tableName + " WHERE " + condition;
            long result = -1;

            dr = (new OleDbCommand(sqlString, cn)).ExecuteReader();

            if (dr.HasRows) {
                dr.Read();
                result = Convert.ToInt64(dr.GetValue(0));
            }

            dr.Close();
            Disconnect();

            return result;
        }
        public long Count(string tableName) {
            return Count(tableName, "");
        }
        public DataTable Query(string sqlString) {
            Connect();
            string tableName = GetTableName(sqlString);
            ds = new DataSet();

            da = new OleDbDataAdapter();
            da.SelectCommand = new OleDbCommand(sqlString, cn);
            da.Fill(ds, tableName);
            Disconnect();

            return ds.Tables[tableName].Copy();
        }
        public object GetField(string sqlString, int fieldIndex) {
            object result;

            cm = new OleDbCommand(sqlString, cn);
            dr = cm.ExecuteReader();

            if (dr.HasRows) {
                dr.Read();

                try {
                    result = dr[fieldIndex];
                } catch {
                    result = 0;
                }
            } else
                result = null;

            dr.Close();

            return result;
        }
        public DataRow GetRow(string sqlString) {
            string tableName = GetTableName(sqlString);
            ds = new DataSet();

            da = new OleDbDataAdapter();
            da.SelectCommand = new OleDbCommand(sqlString, cn);
            da.Fill(ds, tableName);

            return ds.Tables[tableName].Rows.Count == 0 ? null : ds.Tables[tableName].Rows[0];
        }
        public DataTable GetTablesList()    {
            return GetTablesList(null);
        }
        public DataTable GetTablesList(string databaseName) {
            ds = new DataSet();

            da = new OleDbDataAdapter();
            da.SelectCommand = new OleDbCommand("SHOW TABLES" + (databaseName != null ? " FROM " + databaseName : "") + ";", cn);
            da.Fill(ds, "TablesList");

            return ds.Tables["TablesList"].Copy();
        }
        public DataTable GetTable(string tableName) {
            ds = new DataSet();

            da = new OleDbDataAdapter();
            da.SelectCommand = new OleDbCommand("SELECT * FROM " + tableName, cn);
            da.Fill(ds, tableName);

            return ds.Tables[tableName].Copy();
        }

        // Insert, Delete and Update

        public void ExecuteNonQuery(string sqlString) {
            cm = new OleDbCommand(sqlString, cn);
            cm.ExecuteNonQuery();
        }
        public void Insert(string tableName, ArrayList columns, ArrayList row) {
            string sqlInsert;

            sqlInsert = "INSERT INTO " + tableName + " (" + GetInsertColumnsList(columns) + ") VALUES (" + GetInsertValuesList(columns) + ")";

            cm = new OleDbCommand(sqlInsert, cn);

            for (int i = 0; i < columns.Count; i++) {
                cm.Parameters.Add("@" + (string)columns[i], row[i]);
            }

            cm.ExecuteNonQuery();
        }
        public void Delete(string tableName) {
            Delete(tableName, "");
        }
        public void Delete(string tableName, string condition) {
            string sqlDelete = "DELETE FROM " + tableName + (condition.Trim() == "" ? "" : " WHERE " + condition);
            (new OleDbCommand(sqlDelete, cn)).ExecuteNonQuery();
        }
        public void Update(string tableName, ArrayList columns, ArrayList row) {
            Update(tableName, columns, row, "");
        }
        public void Update(string tableName, ArrayList columns, ArrayList row, string condition) {
            string sqlUpdate = "";

            sqlUpdate = "UPDATE " + tableName + " SET " + GetUpdateList(columns) + (condition.Trim() == "" ? "" : " WHERE " + condition);
            cm = new OleDbCommand(sqlUpdate, cn);

            for (int i = 0; i < columns.Count; i++) {
                cm.Parameters.Add("@" + (string)columns[i], row[i]);
            }

            cm.ExecuteNonQuery();
        }

        // Columns and rows

        public static OleDbParameter CloneParameter(OleDbParameter p) {
            OleDbParameter param = new OleDbParameter(p.ParameterName, p.OleDbType);

            param.SourceColumn = p.SourceColumn;
            param.SourceVersion = p.SourceVersion;
            param.Size = p.Size;

            return param;
        }
        public static OleDbParameter NewParameter(string name, OleDbType type, string sourceColumn, DataRowVersion sourceVersion) {
            OleDbParameter param = new OleDbParameter(name, type);

            param.SourceColumn = sourceColumn;
            param.SourceVersion = sourceVersion;
            param.SourceVersion = sourceVersion;

            return param;
        }
        public static OleDbParameter NewParameter(string name, OleDbType type, int size, string sourceColumn, DataRowVersion sourceVersion) {
            OleDbParameter param = new OleDbParameter(name, type, size, sourceColumn);
            param.SourceVersion = sourceVersion;

            return param;
        }
        protected static string GetInsertColumnsList(ArrayList columns) {
            string list = "";

            foreach (object o in columns) {
                list += (string)o + ", ";
            }

            return list.Substring(0, list.Length - 2);
        }
        protected static string GetInsertValuesList(ArrayList columns) {
            string list = "";

            for (int i = 0; i < columns.Count; i++) {
                list += "?, ";
            }

            return list.Substring(0, list.Length - 2);
        }
        protected static string GetUpdateList(ArrayList columns) {
            string list = "";

            foreach (object o in columns) {
                list += (string)o + "=?, ";
            }

            return list.Substring(0, list.Length - 2);
        }
        protected static void CopyColumnsNames(DataTable t, ArrayList columns) {
            foreach (DataColumn c in t.Columns) {
                columns.Add(c.ColumnName);
            }
        }
        protected static void CopyColumns(DataView source, DataTable dest) {
            foreach (DataColumn c in source.Table.Columns) {
                dest.Columns.Add(c.ColumnName, c.DataType, c.Expression);
            }
        }
        protected static void CopyRows(DataView source, DataTable dest) {
            foreach (DataRowView r in source) {
                dest.Rows.Add(r.Row.ItemArray);
            }
        }
        protected static string BuildCondition(string fieldName, System.Type theType) {
            string condition = fieldName + "=";

            if (theType == typeof(string) || theType == typeof(DateTime)) {
                condition += "'{0}'";
            } else {
                condition += "{0}";
            }

            return condition;
        }

    }
}
