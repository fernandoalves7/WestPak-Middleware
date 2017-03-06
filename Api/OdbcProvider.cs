using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WestPakMiddleware.Api {
    public sealed class OdbcProvider : Database {
        private DataSet ds;
        private DataView dv;
        private OdbcConnection cn;
        private OdbcCommand cm;
        private OdbcDataReader dr;
        private OdbcDataAdapter da;
        private OdbcTransaction tr;
        private OdbcCommandBuilder cb;
        private string dsn;

        private string sqlBulkInsertTemplate;
        private string sqlBulkInsertValuesTemplate;

        public OdbcProvider(string dsn) : base() {
            this.dsn = dsn;
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
            cn = new OdbcConnection("DSN=" + dsn);
            //cn = new OdbcConnection("Driver={Microsoft Access Driver (*.accdb)};DBQ=D:\\Program Files\\Microsoft Office\\Office10\\Samples\\Northwind.mdb;UID=;PWD=;");
            //cn = new OdbcConnection(@"Driver={Microsoft Access Driver(*.mdb, *.accdb)};DBQ=C:\Fernando\Work\Companies\RCO Services\Growing Project\Growers Bin Estimate (Access)\Growers Bin Estimate_be.accdb;");
            cn.Open();
        }
        public void Disconnect() {
            if (cn != null || cn.State != ConnectionState.Closed) {
                cn.Close();
                cn = null;
            }
        }
        public void TestConnection(string tableName) {
            try {
                Query("SELECT TOP 1 * FROM " + tableName + "");
            } catch (Exception ex) {
                Query("SELECT * FROM " + tableName + "");
            }
        }

        // Select

        public bool Exists(string sqlString) {
            bool result;

            Connect();
            cm = new OdbcCommand(sqlString, cn);
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

            dr = (new OdbcCommand(sqlString, cn)).ExecuteReader();

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

            da = new OdbcDataAdapter();
            da.SelectCommand = new OdbcCommand(sqlString, cn);
            da.Fill(ds, tableName);
            Disconnect();

            return ds.Tables[tableName].Copy();
        }
        public OdbcDataReader OpenTableReader(string tablename) {
            return OpenTableReader(tablename, null);
        }
        public OdbcDataReader OpenTableReader(string tablename, string whereClause) {
            this.cm = new OdbcCommand("SELECT * FROM " + tablename + (whereClause == null ? "" : " WHERE " + whereClause), this.cn);
            this.dr = this.cm.ExecuteReader(CommandBehavior.SequentialAccess);
            //this.cm.Dispose();

            return this.dr;
        }
        public OdbcDataReader OpenQueryReader(string sqlString) {
            this.cm = new OdbcCommand(sqlString, this.cn);
            this.dr = this.cm.ExecuteReader(CommandBehavior.SequentialAccess);
            //this.cm.Dispose();

            return this.dr;
        }
        public object GetField(string sqlString, int fieldIndex) {
            object result;

            cm = new OdbcCommand(sqlString, cn);
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

            da = new OdbcDataAdapter();
            da.SelectCommand = new OdbcCommand(sqlString, cn);
            da.Fill(ds, tableName);

            return ds.Tables[tableName].Rows.Count == 0 ? null : ds.Tables[tableName].Rows[0];
        }
        public DataTable GetTablesList() {
            return GetTablesList(null);
        }
        public DataTable GetTablesList(string databaseName) {
            ds = new DataSet();

            da = new OdbcDataAdapter();
            da.SelectCommand = new OdbcCommand("SHOW TABLES" + (databaseName != null ? " FROM " + databaseName : "") + ";", cn);
            da.Fill(ds, "TablesList");

            return ds.Tables["TablesList"].Copy();
        }
        public DataTable GetTable(string tableName) {
            ds = new DataSet();

            da = new OdbcDataAdapter();
            da.SelectCommand = new OdbcCommand("SELECT * FROM " + tableName, cn);
            da.Fill(ds, tableName);

            return ds.Tables[tableName].Copy();
        }

        // Insert, Delete and Update

        public void ExecuteNonQuery(string sqlString) {
            cm = new OdbcCommand(sqlString, cn);
            cm.ExecuteNonQuery();
        }
        public void Insert(string tableName, ArrayList columns, ArrayList row) {
            string sqlInsert;

            sqlInsert = "INSERT INTO " + tableName + " (" + GetInsertColumnsList(columns) + ") VALUES (" + GetInsertValuesList(columns) + ")";

            cm = new OdbcCommand(sqlInsert, cn);

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
            (new OdbcCommand(sqlDelete, cn)).ExecuteNonQuery();
        }
        public void Update(string tableName, ArrayList columns, ArrayList row) {
            Update(tableName, columns, row, "");
        }
        public void Update(string tableName, ArrayList columns, ArrayList row, string condition) {
            string sqlUpdate = "";

            sqlUpdate = "UPDATE " + tableName + " SET " + GetUpdateList(columns) + (condition.Trim() == "" ? "" : " WHERE " + condition);
            cm = new OdbcCommand(sqlUpdate, cn);

            for (int i = 0; i < columns.Count; i++) {
                cm.Parameters.Add("@" + (string)columns[i], row[i]);
            }

            cm.ExecuteNonQuery();
        }

        // Bulk inserts

        public static string ScapeSqlCharacters(string insertField) {
            return insertField.Replace(@"\", @"\\").Replace("\'", "\\'").Replace("%", "\\%");
        }
        public void SetBulkInsertSqlTemplate(string databasename, string tablename, IEnumerable<string> fields, IEnumerable<Type> fieldsTypes) {
            var fieldsList = new StringBuilder();
            var fieldCounter = 0;
            var needle = "";

            foreach (var field in fields) {
                fieldsList.Append("`").Append(field).Append("`,");
            }

            sqlBulkInsertTemplate = "INSERT INTO " + databasename + "." + tablename + " (" + fieldsList.ToString().TrimEnd(',') + ") VALUES ";
            sqlBulkInsertValuesTemplate = "";

            foreach (Type fieldType in fieldsTypes) {
                switch (fieldType.ToString()) {
                    case "System.String":
                        needle = "'";
                        break;
                }

                sqlBulkInsertValuesTemplate += needle + "@variable" + fieldCounter.ToString() + needle + ",";
                fieldCounter++;
            }

            sqlBulkInsertValuesTemplate = "(" + this.sqlBulkInsertValuesTemplate.TrimEnd(',') + "),";
        }
        public void WriteBulk(string databasename, List<List<string>> rows) {
            var result = new StringBuilder();

            result.Append(this.sqlBulkInsertTemplate);

            foreach (var entry in rows) {
                result.Append(PopulateValuesEntry(entry));
            }

            ExecuteNonQuery(result.ToString().TrimEnd(','));
        }
        private string PopulateValuesEntry(IEnumerable<string> entryValues) {
            var insertEntry = this.sqlBulkInsertValuesTemplate;
            var fieldCounter = 0;

            foreach (var value in entryValues) {
                insertEntry = insertEntry.Replace("@variable" + fieldCounter.ToString() + "'", ScapeSqlCharacters(value) + "'");
                fieldCounter++;
            }

            return insertEntry;
        }

        // Transactions

        public int UpdateTable(string tableName, DataTable t) {
            int result = -1;

            Connect();
            da = new OdbcDataAdapter("SELECT * FROM " + tableName, cn);
            cb = new OdbcCommandBuilder(da);
            result = da.Update(t);
            Disconnect();
            t.AcceptChanges();

            return result;
        }
        public int UpdateQuery(string sqlString, DataTable t) {
            int result = -1;

            Connect();
            da = new OdbcDataAdapter(sqlString, cn);
            cb = new OdbcCommandBuilder(da);
            result = da.Update(t);
            Disconnect();

            t.AcceptChanges();

            return result;
        }
        public int UpdateQuery(string sqlString, OdbcCommand update, OdbcCommand insert, OdbcCommand delete, DataTable t) {
            int result = -1;

            Connect();
            da = new OdbcDataAdapter(sqlString, cn);

            update.Connection = cn;
            insert.Connection = cn;
            delete.Connection = cn;

            da.UpdateCommand = update;
            da.InsertCommand = insert;
            da.DeleteCommand = delete;
            result = da.Update(t);
            Disconnect();
            t.AcceptChanges();

            return result;
        }
        public void Insert(DataTable t, string tableName) {
            ArrayList columns = new ArrayList(), row = new ArrayList();

            foreach (DataColumn c in t.Columns) {
                columns.Add(c.ColumnName);
            }

            Connect();

            foreach (DataRow r in t.Rows) {
                row.Clear();

                for (int i = 0; i < t.Columns.Count; i++) {
                    row.Add(r[i]);
                }

                Insert(tableName, columns, row);
            }

            Disconnect();
        }
        public void BeginTransaction() {
            Connect();
            cm = new OdbcCommand();
            tr = cn.BeginTransaction();
            cm.Transaction = tr;
        }
        public void EndTransaction() {
            tr.Commit();
            Disconnect();
        }
        public void AbortTransaction() {
            tr.Rollback();
            Disconnect();
        }
        public void SetTransactionConstraints(string mode) {
            cm.CommandType = CommandType.Text;
            cm.CommandText = "SET CONSTRAINTS " + mode;
            cm.Connection = cn;
            cm.ExecuteScalar();
        }
        public void TransactionUpdate(string tableName, ArrayList columns, ArrayList row, string condition) {
            OdbcParameterCollection parameters;
            string sqlUpdate = "";

            sqlUpdate = "UPDATE " + tableName + " SET " + GetUpdateList(columns) + (condition.Trim() == "" ? "" : " WHERE " + condition);
            parameters = cm.Parameters;

            for (int i = 0; i < columns.Count; i++) {
                cm.Parameters.Add("@" + (string)columns[i], row[i]);
            }

            CommandExecute(sqlUpdate);
        }
        public void TransactionUpdate(string tableName, ArrayList columns, ArrayList row) {
            TransactionUpdate(tableName, columns, row, "");
        }
        public void TransactionUpdate(string tableName, string condition, DataRow r) {
            TransactionUpdate(tableName, GetColumnsNames(r), GetRowFields(r), "");
        }
        public void TransactionInsert(string tableName, DataTable t) {
            ArrayList columns = new ArrayList(), row = new ArrayList();

            foreach (DataColumn c in t.Columns) {
                columns.Add(c.ColumnName);
            }

            foreach (DataRow r in t.Rows) {
                row.Clear();

                for (int i = 0; i < t.Columns.Count; i++) {
                    row.Add(r[i]);
                }

                TransactionInsert(tableName, columns, row);
            }
        }
        public void TransactionInsert(string tableName, ArrayList columns, ArrayList row) {
            string sqlInsert;

            sqlInsert = "INSERT INTO " + tableName + " (" + GetInsertColumnsList(columns) + ") VALUES (" + GetInsertValuesList(columns) + ")";

            cm.Parameters.Clear();

            for (int i = 0; i < columns.Count; i++) {
                cm.Parameters.Add("@" + (string)columns[i], row[i]);
            }

            CommandExecute(sqlInsert);
        }
        public void TransactionInsert(string tableName, string condition, DataRow r) {
            TransactionInsert(tableName, GetColumnsNames(r), GetRowFields(r));
        }
        public void TransactionDelete(string tableName, string condition) {
            CommandExecute("DELETE FROM " + tableName + (condition.Trim() == "" ? "" : " WHERE " + condition));
        }
        public void TransactionDelete(string tableName) {
            TransactionDelete(tableName, "");
        }
        public void TransactionDeleteAndInsert(DataTable t, string tableName, string deleteCondition) {
            BeginTransaction();

            try {
                TransactionDelete(tableName, deleteCondition);
                TransactionInsert(tableName, t);
            } catch (Exception ex) {
                AbortTransaction();
                throw (ex);
            }

            EndTransaction();
        }
        public void TransactionInsert(DataTable t, string tableName) {
            BeginTransaction();

            try {
                TransactionInsert(tableName, t);
            } catch (Exception ex) {
                AbortTransaction();
                throw (ex);
            }

            EndTransaction();
        }
        protected void CommandExecute(string sqlString) {
            cm.CommandType = CommandType.Text;
            cm.CommandText = sqlString;
            cm.Connection = cn;
            cm.Transaction = tr;
            cm.ExecuteNonQuery();
        }

        // Columns and rows

        public static OdbcParameter CloneParameter(OdbcParameter p) {
            OdbcParameter param = new OdbcParameter(p.ParameterName, p.OdbcType);

            param.SourceColumn = p.SourceColumn;
            param.SourceVersion = p.SourceVersion;
            param.Size = p.Size;

            return param;
        }
        public static OdbcParameter NewParameter(string name, OdbcType type, string sourceColumn, DataRowVersion sourceVersion) {
            OdbcParameter param = new OdbcParameter(name, type);

            param.SourceColumn = sourceColumn;
            param.SourceVersion = sourceVersion;
            param.SourceVersion = sourceVersion;

            return param;
        }
        public static OdbcParameter NewParameter(string name, OdbcType type, string sourceColumn) {
            return NewParameter(name, type, sourceColumn, DataRowVersion.Original);
        }
        public static OdbcParameter NewParameter(string name, OdbcType type, int size, string sourceColumn) {
            return NewParameter(name, type, sourceColumn, DataRowVersion.Original);
        }
        public static OdbcParameter NewParameter(string name, OdbcType type, int size, string sourceColumn, DataRowVersion sourceVersion) {
            OdbcParameter param = new OdbcParameter(name, type, size, sourceColumn);
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

        // State

        public static bool Exists(DataTable t, string field, int columnIndex) {
            foreach (DataRow r in t.Rows) {
                if ((string)r[columnIndex] == field) {
                    return true;
                }
            }

            return false;
        }
        public static bool ExistsNullField(DataTable t, int startIndex) {
            DataView dv;

            dv = new DataView(t);
            dv.RowStateFilter = DataViewRowState.CurrentRows;

            foreach (DataRowView r in dv) {
                for (int i = startIndex; i < t.Columns.Count; i++) {
                    if (r[i] is DBNull) {
                        return true;
                    }
                }
            }

            return false;
        }
        public static bool ExistsNullField(DataTable t, int startIndex, int length) {
            DataView dv;

            dv = new DataView(t);
            dv.RowStateFilter = DataViewRowState.CurrentRows;

            foreach (DataRowView r in dv) {
                for (int i = startIndex; i < startIndex + length; i++) {
                    if (r[i] is DBNull) {
                        return true;
                    }
                }
            }

            return false;
        }
        public static bool ExistsDuplicatedValuesString(DataTable t, int columnIndex) {
            for (int i = 0; i < t.Rows.Count; i++) {
                DataRow a = t.Rows[i];

                if (a.RowState == DataRowState.Deleted) {
                    continue;
                }

                for (int j = 0; j < t.Rows.Count; j++) {
                    DataRow b = t.Rows[j];

                    if (b.RowState == DataRowState.Deleted) {
                        continue;
                    }

                    string valueAtColumnA = (string)a[columnIndex];
                    string valueAtColumnB = (string)b[columnIndex];

                    if (i != j && valueAtColumnA.CompareTo(valueAtColumnB) == 0) {
                        return true;
                    }
                }
            }

            return false;
        }

        // Read

        public static string GetTableName(string sqlString) {
            string tableName;

            tableName = sqlString.Substring(sqlString.ToUpper().IndexOf("FROM ") + 5);

            if (tableName.ToUpper().IndexOf(" ") >= 0) {
                tableName = tableName.Substring(0, tableName.ToUpper().IndexOf(" "));
            }

            return tableName;
        }
        public static string GetColumnName(DataTable t, int fieldIndex) {
            return t.Columns[fieldIndex].ColumnName;
        }
        public static ArrayList GetColumnsNames(DataRow r) {
            ArrayList columns = new ArrayList();

            foreach (DataColumn c in r.Table.Columns) {
                columns.Add(c.ColumnName);
            }

            return columns;
        }
        public static ArrayList GetRowFields(DataRow r) {
            ArrayList fields = new ArrayList();

            foreach (DataColumn c in r.Table.Columns) {
                fields.Add(r[c]);
            }

            return fields;
        }
        public static DataRow GetRow(DataTable t, string field, int columnIndex) {
            foreach (DataRow r in t.Rows) {
                if (((string)r[columnIndex]) == field) {
                    return r;
                }
            }

            return null;
        }
        public static DataRow GetRow(DataTable t, int field, int columnIndex) {
            foreach (DataRow r in t.Rows) {
                if (r.RowState == DataRowState.Deleted) {
                    continue;
                }

                if (Convert.ToInt32(r[columnIndex]) == field) {
                    return r;
                }
            }

            return null;
        }
        public static DataRow GetRandomRow(DataTable t) {
            return t.Rows[(new Random()).Next(0, t.Rows.Count)];
        }
        public static ArrayList GetColumns(DataTable t) {
            ArrayList l = new ArrayList();

            foreach (DataColumn c in t.Columns) {
                l.Add(c.ColumnName);
            }

            return l;
        }
        public static object GetField(DataTable t, string comparationValue, int comparationIndex, int resultIndex) {
            foreach (DataRow r in t.Rows) {
                if (((string)r[comparationIndex]) == comparationValue) {
                    return r[resultIndex];
                }
            }

            return null;
        }
        public static object GetField(DataTable t, int comparationValue, int comparationIndex, int resultIndex) {
            foreach (DataRow r in t.Rows) {
                if (Convert.ToInt32(r[comparationIndex]) == comparationValue) {
                    return r[resultIndex];
                }
            }

            return null;
        }
        public static DataTable GetTable(DataTable t, string field, int comparationIndex) {
            DataTable t2 = new DataTable(t.TableName);

            foreach (DataColumn c in t.Columns) {
                t2.Columns.Add(c.ColumnName, c.DataType, c.Expression);
            }

            foreach (DataRow r in t.Rows) {
                if (((string)r[comparationIndex]) == field) {
                    t2.Rows.Add(r.ItemArray);
                }
            }

            return t2;
        }
        public static DataTable GetTable(DataTable t, int startIndex, int length) {
            DataTable t2 = new DataTable(t.TableName);
            int counter = 0;

            foreach (DataColumn c in t.Columns) {
                t2.Columns.Add(c.ColumnName, c.DataType, c.Expression);
            }

            foreach (DataRow r in t.Rows) {
                if (counter >= startIndex && counter < startIndex + length) {
                    t2.Rows.Add(r.ItemArray);
                }

                counter++;
            }

            return t2;
        }
        public static DataTable GetTable(DataTable t, int startIndex) {
            return GetTable(t, startIndex, t.Rows.Count - startIndex);
        }
        public static DataTable GetTable(DataSet ds, int index) {
            return ds.Tables[index];
        }
        public static DataTable GetTable(DataSet ds) {
            return ds.Tables[0];
        }
        public static DataTable GetTable(DataView dv) {
            DataTable t = new DataTable(dv.Table.TableName);

            CopyColumns(dv, t);
            CopyRows(dv, t);

            return t;
        }
        public static DataSet EncapsulateInDataSet(DataTable t) {
            DataSet ds = new DataSet();
            ds.Tables.Add(t.Copy());

            return ds;
        }
        public static object[] EncapsulateDataRow(DataRow r) {
            object[] columns = new object[r.Table.Columns.Count];

            for (int i = 0; i < r.Table.Columns.Count; i++) {
                columns[i] = r[i];
            }

            return columns;
        }
        public static void OverwriteTableFieldValue(DataTable t, int columnIndex, object columnValue) {
            foreach (DataRow r in t.Rows) {
                if (r.RowState != DataRowState.Deleted) {
                    r[columnIndex] = columnValue;
                }
            }
        }

        // Process

        public static DataTable ComputeTablesUnion(DataTable t1, DataTable t2) {
            DataTable t = new DataTable(t1.TableName);

            foreach (DataColumn c in t1.Columns) {
                t.Columns.Add(c.ColumnName, c.DataType, c.Expression);
            }

            foreach (DataRow r in t1.Rows) {
                t.Rows.Add(r.ItemArray);
            }

            foreach (DataRow r in t2.Rows) {
                t.Rows.Add(r.ItemArray);
            }

            return t;
        }
    }
}
