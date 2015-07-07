using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace DataInspection.Helper
{
    public class SqlExecuteHelper
    {
        private string _connStr;
        public string ConnStr
        {
            set { _connStr = value; }
            get { return _connStr; }
        }

        public SqlExecuteHelper(string connectionString = "")
        {
            ConnStr = connectionString;
        }

        public void Execute(string sql)
        {
            try
            {
                using (SqlConnection sqlconn = new SqlConnection(ConnStr))
                {
                    using (SqlCommand sqlcomm = new SqlCommand(sql, sqlconn))
                    {
                        sqlconn.Open();

                        sqlcomm.ExecuteNonQuery();

                        sqlconn.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public string GetSacalarString(string sql)
        {
            DataTable dt = GetDataTable(sql);

            if (dt.Rows != null && dt.Rows.Count > 0 && dt.Columns != null && dt.Columns.Count > 0)
            {
                return GetObjectValueString(dt.Rows[0][0]);
            }

            return string.Empty;
        }

        public DataTable GetDataTable(string sql)
        {
            try
            {
                using (SqlConnection sqlconn = new SqlConnection(_connStr))
                {
                    using (SqlDataAdapter adapter = new SqlDataAdapter(sql, sqlconn))
                    {
                        DataSet ds = new DataSet();
                        adapter.Fill(ds);

                        if (ds.Tables != null || ds.Tables.Count > 0)
                        {
                            return ds.Tables[0];
                        }
                    }
                }
            }
            catch (SqlException sqlException)
            {
                return null;
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return new DataTable();
        }

        public DataTable GetDataTable(string sql, string tableName, string tableNameSpace)
        {
            try
            {
                using (SqlConnection sqlconn = new SqlConnection(_connStr))
                {
                    using (SqlDataAdapter adapter = new SqlDataAdapter(sql, sqlconn))
                    {
                        DataSet ds = new DataSet();
                        adapter.Fill(ds);
                        if (ds.Tables != null || ds.Tables.Count > 0)
                        {
                            ds.Tables[0].TableName = tableName;
                            ds.Tables[0].Namespace = tableNameSpace;
                            return ds.Tables[0];
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return new DataTable();
        }

        public string GetObjectValueString(object o)
        {
            return o == null ? string.Empty : o.ToString();
        }
    }
}
