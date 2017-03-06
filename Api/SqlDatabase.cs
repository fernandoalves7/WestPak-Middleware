using System.Collections;
using System.Data.SqlClient;
using System.Data;
using System;
using System.Collections.Generic;

namespace WestPakMiddleware
{
    /// <summary>
    /// This class gives access to SQL Server databases via the SqlClient data provider.
    /// 
    /// Methods that ends with no number means that it will only execute the command. If no connection is active it will return error.
    /// Methods that ends with number 2 means that it will connect to the database, execute the command and close the connection.
    /// Methods that ends with number 3 means that belongs to a transaction.
    /// </summary>
    public sealed class SqlDatabase : Database
    {
        private string host, database, username, password;
        private SqlConnection cn;

        private SqlCommand cm;
        private SqlDataReader dr;
        private SqlDataAdapter da;
        private SqlTransaction tr;
        private SqlCommandBuilder cb;

        // Initialization

        public SqlDatabase(string host, string database, string username, string password) : base()
        {
            this.host = host;
            this.database = database;
            this.username = username;
            this.password = password;
        }
        public bool IsConnected()
        {
            if (cn == null)
                return false;
            else
                if (cn.State != ConnectionState.Open)
                    return false;
                else
                    return true;
        }
        public void Connect()
        {
            cn = new SqlConnection("Server=" + host + "; Database=" + database + "; User ID=" + username + "; Password=" + password + "; Trusted_Connection=yes;");
            cn.Open();
        }
        public void Disconnect()
        {
            if (cn != null || cn.State != ConnectionState.Closed)
            {
                cn.Close();
                cn = null;
            }
        }

        // Select

        public bool Exists(string sqlString)
        {
            bool result;

            Connect();
            cm = new SqlCommand(sqlString, cn);
            dr = cm.ExecuteReader();
            result = dr.HasRows;
            dr.Close();
            Disconnect();

            return result;
        }
        public int Count(string tableName)
        {
            return Count(tableName, "");
        }
        public int Count(string tableName, string condition)
        {
            Connect();
            string sqlString = "SELECT COUNT(*) FROM " + tableName + (condition.Trim() == "" ? "" : " WHERE " + condition);
            int result = -1;

            dr = (new SqlCommand(sqlString, cn)).ExecuteReader();

            if (dr.HasRows)
            {
                dr.Read();
                result = Convert.ToInt32(dr.GetValue(0));
            }

            dr.Close();
            Disconnect();

            return result;
        }
        public DataTable Query(string sqlString)
        {
            Connect();
            string tableName = GetTableName(sqlString);
            ds = new DataSet();

            da = new SqlDataAdapter();
            da.SelectCommand = new SqlCommand(sqlString, cn);
            da.Fill(ds, tableName);
            Disconnect();

            return ds.Tables[tableName].Copy();
        }
        public DataTable GetTable(string tableName)
        {
            Connect();
            ds = new DataSet();

            da = new SqlDataAdapter();
            da.SelectCommand = new SqlCommand("SELECT * FROM " + tableName, cn);
            da.Fill(ds, tableName);
            Disconnect();

            return ds.Tables[tableName].Copy();
        }
        public DataRow GetRow(string sqlString)
        {
            Connect();
            string tableName = GetTableName(sqlString);
            ds = new DataSet();

            da = new SqlDataAdapter();
            da.SelectCommand = new SqlCommand(sqlString, cn);
            da.Fill(ds, tableName);
            Disconnect();

            return ds.Tables[tableName].Rows[0];
        }
        public T GetField<T>(string sqlString) {
            return GetField<T>(sqlString, 0);
        }
        public T GetField<T>(string sqlString, int fieldIndex)
        {
            Connect();
            object result;

            dr = (new SqlCommand(sqlString, cn)).ExecuteReader();

            if (dr.HasRows) {
                dr.Read();
                result = dr[fieldIndex];
            } else
                result = null;

            dr.Close();
            Disconnect();

            return (T) result;
        }

        // Update, insert, delete

        public void Update(string tableName, List<string> columns, List<object> row, string condition) {
            Connect();
            string sqlUpdate = "";

            sqlUpdate = "UPDATE " + tableName + " SET " + GetUpdateList(columns) + (condition.Trim() == "" ? "" : " WHERE " + condition);
            cm = new SqlCommand(sqlUpdate, cn);

            for (int i = 0; i < columns.Count; i++)
                cm.Parameters.Add("@" + columns[i], row[i] ?? Convert.DBNull);

            cm.ExecuteNonQuery();
            Disconnect();
        }
        public void Insert(string tableName, ArrayList columns, ArrayList row)
        {
            Connect();
            string sqlInsert;

            sqlInsert = "INSERT INTO " + tableName + " (" + GetInsertColumnsList(columns) + ") VALUES (" + GetInsertValuesList(columns) + ")";
            cm = new SqlCommand(sqlInsert, cn);

            for (int i = 0; i < columns.Count; i++)
                cm.Parameters.Add("@" + (string)columns[i], row[i]);

            cm.ExecuteNonQuery();
            Disconnect();
        }
        public void Insert(string tableName, List<string> columns, List<object> row) {
            Connect();
            string sqlInsert;

            sqlInsert = "INSERT INTO " + tableName + " (" + GetInsertColumnsList(columns) + ") VALUES (" + GetInsertValuesList(columns) + ")";
            cm = new SqlCommand(sqlInsert, cn);

            for (int i = 0; i < columns.Count; i++)
                cm.Parameters.Add("@" + columns[i], row[i] ?? Convert.DBNull);

            cm.ExecuteNonQuery();
            Disconnect();
        }
        public void Delete(string tableName)
        {
            Delete(tableName, "");
        }
        public void Delete(string tableName, string condition)
        {
            Connect();
            string sqlDelete = "DELETE FROM " + tableName + (condition.Trim() == "" ? "" : " WHERE " + condition);
            (new SqlCommand(sqlDelete, cn)).ExecuteNonQuery();
            Disconnect();
        }

        // Transactions

        public int UpdateTable(string tableName, DataTable t)
        {
            int result = -1;

            Connect();
            da = new SqlDataAdapter("SELECT * FROM " + tableName, cn);
            cb = new SqlCommandBuilder(da);
            result = da.Update(t);
            Disconnect();

            t.AcceptChanges();

            return result;
        }
        public int UpdateQuery(string sqlString, DataTable t)
        {
            int result = -1;

            Connect();
            da = new SqlDataAdapter(sqlString, cn);
            cb = new SqlCommandBuilder(da);
            result = da.Update(t);
            Disconnect();

            t.AcceptChanges();

            return result;
        }
        public int UpdateQuery(string sqlString, SqlCommand update, SqlCommand insert, SqlCommand delete, DataTable t)
        {
            int result = -1;

            Connect();
            da = new SqlDataAdapter(sqlString, cn);

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
        public void Insert(string tableName, DataTable t)
        {
            ArrayList columns = new ArrayList();
            CopyColumnsNames(t, columns);

            BeginTransaction();
            try
            {
                foreach (DataRow r in t.Rows)
                    Insert3(tableName, columns, GetRowFields(r));

                EndTransaction();
            }
            catch
            {
                AbortTransaction();
            }
        }
        public void BeginTransaction()
        {
            Connect();
            cm = new SqlCommand();
            tr = cn.BeginTransaction();
            cm.Transaction = tr;
        }
        public void EndTransaction()
        {
            tr.Commit();
            Disconnect();
        }
        public void AbortTransaction()
        {
            tr.Rollback();
            Disconnect();
        }
        public void SetTransactionConstraints(string mode)
        {
            cm.CommandType = CommandType.Text;
            cm.CommandText = "SET CONSTRAINTS " + mode;
            cm.Connection = cn;
            cm.ExecuteScalar();
        }
        public void Update3(string tableName, ArrayList columns, ArrayList row, string condition)
        {
            SqlParameterCollection parameters;
            string sqlUpdate = "";

            sqlUpdate = "UPDATE " + tableName + " SET " + GetUpdateList(columns) + (condition.Trim() == "" ? "" : " WHERE " + condition);
            parameters = cm.Parameters;

            for (int i = 0; i < columns.Count; i++)
                cm.Parameters.Add("@" + (string)columns[i], row[i]);

            CommandExecute(sqlUpdate);
        }
        public void Update3(string tableName, ArrayList columns, ArrayList row)
        {
            Update3(tableName, columns, row, "");
        }
        public void Update3(string tableName, string condition, DataRow r)
        {
            Update3(tableName, GetColumnsNames(r), GetRowFields(r), "");
        }
        public void Insert3(string tableName, ArrayList columns, ArrayList row)
        {
            string sqlInsert;

            sqlInsert = "INSERT INTO " + tableName + " (" + GetInsertColumnsList(columns) + ") VALUES (" + GetInsertValuesList(columns) + ")";
            cm.Parameters.Clear();

            for (int i = 0; i < columns.Count; i++)
                cm.Parameters.Add("@" + (string)columns[i], row[i]);

            CommandExecute(sqlInsert);
        }
        public void Insert3(string tableName, string condition, DataRow r)
        {
            Insert3(tableName, GetColumnsNames(r), GetRowFields(r));
        }
        public void Delete3(string tableName, string condition)
        {
            CommandExecute("DELETE FROM " + tableName + (condition.Trim() == "" ? "" : " WHERE " + condition));
        }
        public void Delete3(string tableName)
        {
            Delete3(tableName, "");
        }

        // Generic use

        public static SqlParameter NewParameter(string name, SqlDbType type, string sourceColumn, DataRowVersion sourceVersion)
        {
            SqlParameter param = new SqlParameter(name, type);

            param.SourceColumn = sourceColumn;
            param.SourceVersion = sourceVersion;
            param.SourceVersion = sourceVersion;

            return param;
        }
        public static SqlParameter NewParameter(string name, SqlDbType type, string sourceColumn)
        {
            return NewParameter(name, type, sourceColumn, DataRowVersion.Original);
        }
        public static SqlParameter NewParameter(string name, SqlDbType type, int size, string sourceColumn, DataRowVersion sourceVersion)
        {
            SqlParameter param = new SqlParameter(name, type, size, sourceColumn);

            param.SourceVersion = sourceVersion;

            return param;
        }
        public static SqlParameter NewParameter(string name, SqlDbType type, int size, string sourceColumn)
        {
            return NewParameter(name, type, sourceColumn, DataRowVersion.Original);
        }
        public static SqlParameter CloneParameter(SqlParameter p)
        {
            SqlParameter param = new SqlParameter(p.ParameterName, p.SqlDbType);

            param.SourceColumn = p.SourceColumn;
            param.SourceVersion = p.SourceVersion;
            param.Size = p.Size;

            return param;

        }
        private static string GetInsertColumnsList(List<string> columns) {
            string list = "";

            foreach (string c in columns)
                list += c + ", ";

            return list.Substring(0, list.Length - 2);
        }
        private static string GetInsertValuesList(List<string> columns) {
            string list = "";

            foreach (string c in columns)
                list += "@" + c + ", ";

            return list.Substring(0, list.Length - 2);
        }
        private static string GetInsertColumnsList(ArrayList columns) {
            string list = "";

            foreach (object o in columns)
                list += (string)o + ", ";

            return list.Substring(0, list.Length - 2);
        }
        private static string GetInsertValuesList(ArrayList columns)
        {
            string list = "";

            foreach (object o in columns)
                list += "@" + (string)o + ", ";

            return list.Substring(0, list.Length - 2);
        }
        private static string GetUpdateList(List<string> columns) {
            string list = "";

            foreach (string c in columns)
                list += c + "=@" + c + ", ";

            return list.Substring(0, list.Length - 2);
        }
        private static string GetUpdateList(ArrayList columns) {
            string list = "";

            foreach (object o in columns)
                list += (string) o + "=@" + (string)o + ", ";

            return list.Substring(0, list.Length - 2);
        }
        private void CommandExecute(string sqlString)
        {
            cm.CommandType = CommandType.Text;
            cm.CommandText = sqlString;
            cm.Connection = cn;
            cm.ExecuteNonQuery();
        }
    }
}