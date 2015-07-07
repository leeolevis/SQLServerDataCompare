using System;
using System.Configuration;
using System.Data;
using System.IO;
using DataInspection.Helper;

namespace DataInspection
{
    internal class CompareEntity
    {
        public CompareEntity(string tableEnName, string tableCnName, string compareType, string hightProp,
            string hightValue, string lowProp, string lowValue, string propName)
        {
            TableEnName = tableEnName;
            TableCnName = tableCnName;
            CompareType = compareType;
            HightProp = hightProp;
            HightValue = hightValue;
            LowProp = lowProp;
            LowValue = lowValue;
            PropName = propName;
        }

        private string TableEnName { get; set; }
        private string TableCnName { get; set; }
        private string CompareType { get; set; }
        private string HightProp { get; set; }
        private string HightValue { get; set; }
        private string LowProp { get; set; }
        private string LowValue { get; set; }
        private string PropName { get; set; }
    }

    internal class Program
    {
        private static void Main(string[] args)
        {
            var compareResult = new DataTable();
            compareResult.Columns.Add(new DataColumn("表英文名", Type.GetType("System.String")));
            compareResult.Columns.Add(new DataColumn("表中文名", Type.GetType("System.String")));
            compareResult.Columns.Add(new DataColumn("比较类型", Type.GetType("System.String")));
            compareResult.Columns.Add(new DataColumn("高版本属性", Type.GetType("System.String")));
            compareResult.Columns.Add(new DataColumn("高版本值", Type.GetType("System.String"))); //删除0/增加1
            compareResult.Columns.Add(new DataColumn("低版本属性", Type.GetType("System.String")));
            compareResult.Columns.Add(new DataColumn("低版本值", Type.GetType("System.String")));
            compareResult.Columns.Add(new DataColumn("属性名称", Type.GetType("System.String")));

            var sqlHigh = new SqlExecuteHelper(ConfigurationManager.AppSettings["highSQLConnect"]);
            var sqlLow = new SqlExecuteHelper(ConfigurationManager.AppSettings["lowSQLConnect"]);

            string tableName = ConfigurationManager.AppSettings["CompareTable"];
            string[] tableArrary = tableName.Split(',');

            var dtHight = new DataTable();
            var dtLow = new DataTable();
            foreach (string currentTable in tableArrary)
            {
                dtHight =
                    sqlHigh.GetDataTable(string.Format(
                        "select * from information_schema.columns where table_name='{0}'", currentTable));

                dtLow =
                    sqlLow.GetDataTable(string.Format(
                        "select * from information_schema.columns where table_name='{0}'", currentTable));

                string currentTableCn = sqlHigh.GetSacalarString(
                    string.Format("SELECT TOP 1 table_name_c  FROM dbo.data_dict WHERE table_name='{0}'", currentTable));

                bool isDiff = true;
                //如果两边都存在
                if (dtHight.Rows.Count != 0 && dtLow.Rows.Count != 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("两边都存在表{0}", currentTable);

                    Console.ForegroundColor = ConsoleColor.White;
                    //todo
                    //高版本的字段
                    for (int i = 0; i < dtHight.Rows.Count; i++)
                    {
                        string currentProp = dtHight.Rows[i]["COLUMN_NAME"].ToString();
                        string sqlProp =
                            string.Format(
                                "select * from information_schema.columns where table_name='{0}' AND COLUMN_NAME='{1}'",
                                currentTable, currentProp);
                        string currentPropCn =
                            sqlHigh.GetSacalarString(
                                string.Format(
                                    "SELECT TOP 1 field_name_c FROM dbo.data_dict WHERE table_name='{0}' AND field_name='{1}'",
                                    currentTable, currentProp));


                        DataTable tempHighTable = sqlHigh.GetDataTable(sqlProp);
                        DataTable tempLowTable = sqlLow.GetDataTable(sqlProp);
                        //如果低版本没有则不比较   
                        if (tempLowTable.Rows.Count < 1)
                        {
                            Console.WriteLine("低版本没有字段{0}", currentProp);
                            //compareResult.Add(new CompareEntity(currentTable, currentTableCn, "低版本没有字段", "", "", "", "", ""));

                            compareResult.Rows.Add(currentTable, currentTableCn, "低版本没有字段", currentProp,
                                tempHighTable.Rows[0]["DATA_TYPE"], "", "", currentPropCn);
                            Console.ForegroundColor = ConsoleColor.White;
                            isDiff = false;
                        }
                        else
                        {
                            //默认值
                            if (tempHighTable.Rows[0]["COLUMN_DEFAULT"].ToString() !=
                                tempLowTable.Rows[0]["COLUMN_DEFAULT"].ToString())
                            {
                                Console.WriteLine("字段{0}高版本默认值为{1}，低版本//默认值为{2}", currentProp,
                                    tempHighTable.Rows[0]["COLUMN_DEFAULT"], tempLowTable.Rows[0]["COLUMN_DEFAULT"]);
                                compareResult.Rows.Add(currentTable, currentTableCn, "默认值", currentProp,
                                    tempHighTable.Rows[0]["COLUMN_DEFAULT"].ToString(),
                                    currentProp, tempLowTable.Rows[0]["COLUMN_DEFAULT"].ToString(), currentPropCn);
                                isDiff = false;
                            }

                            //类型
                            if (tempHighTable.Rows[0]["DATA_TYPE"].ToString() !=
                                tempLowTable.Rows[0]["DATA_TYPE"].ToString())
                            {
                                Console.WriteLine("字段{0}高版本类型为{1}，低版本类型为{2}", currentProp,
                                    tempHighTable.Rows[0]["DATA_TYPE"], tempLowTable.Rows[0]["DATA_TYPE"]);
                                compareResult.Rows.Add(currentTable, currentTableCn, "类型", currentProp,
                                    tempHighTable.Rows[0]["DATA_TYPE"].ToString(),
                                    currentProp, tempLowTable.Rows[0]["DATA_TYPE"].ToString(), currentPropCn);
                                isDiff = false;
                            }

                            //长度
                            if (tempHighTable.Rows[0]["CHARACTER_MAXIMUM_LENGTH"].ToString() !=
                                tempLowTable.Rows[0]["CHARACTER_MAXIMUM_LENGTH"].ToString())
                            {
                                Console.WriteLine("字段{0}高版本类型为{1}，低版本类型为{2}", currentProp,
                                    tempHighTable.Rows[0]["CHARACTER_MAXIMUM_LENGTH"],
                                    tempLowTable.Rows[0]["CHARACTER_MAXIMUM_LENGTH"]);
                                compareResult.Rows.Add(currentTable, currentTableCn, "长度", currentProp,
                                    tempHighTable.Rows[0]["CHARACTER_MAXIMUM_LENGTH"].ToString(),
                                    currentProp, tempLowTable.Rows[0]["CHARACTER_MAXIMUM_LENGTH"].ToString(),
                                    currentPropCn);
                                isDiff = false;
                            }
                        }
                    }

                    if (isDiff)
                        compareResult.Rows.Add(currentTable, currentTableCn, "两边完全相同的表", "", "", "", "", "");

                    //todo
                    //低版本的字段
                    for (int i = 0; i < dtLow.Rows.Count; i++)
                    {
                        string currentProp = dtLow.Rows[i]["COLUMN_NAME"].ToString();
                        string sqlProp =
                            string.Format(
                                "select * from information_schema.columns where table_name='{0}' AND COLUMN_NAME='{1}'",
                                currentTable, currentProp);

                        string currentPropCn =
                            sqlLow.GetSacalarString(
                                string.Format(
                                    "SELECT TOP 1 field_name_c FROM dbo.data_dict WHERE table_name='{0}' AND field_name='{1}'",
                                    currentTable, currentProp));

                        DataTable tempHighTable = sqlHigh.GetDataTable(sqlProp);
                        DataTable tempLowTable = sqlLow.GetDataTable(sqlProp);
                        //如果高版本没有则不比较
                        if (tempHighTable.Rows.Count < 1)
                        {
                            Console.WriteLine("高本没有字段{0}", currentProp);
                            compareResult.Rows.Add(currentTable, currentTableCn, "高版本没有字段", "", "", currentProp,
                                tempLowTable.Rows[0]["DATA_TYPE"], currentPropCn);
                        }
                    }
                }
                    //如果高版本存在，低版本不存在
                else if (dtHight.Rows.Count > 0 && dtLow.Rows.Count == 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("低版本不存在表{0}", currentTable);
                    compareResult.Rows.Add(currentTable, currentTableCn, "低版本不存在表", "", "", "", "", "");
                    Console.ForegroundColor = ConsoleColor.White;
                }
                else if (dtHight.Rows.Count == 0 && dtLow.Rows.Count > 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("高版本不存在表{0}", currentTable);
                    compareResult.Rows.Add(currentTable, currentTableCn, "高版本不存在表", "", "", "", "", "");
                    Console.ForegroundColor = ConsoleColor.White;
                }
                else if (dtHight.Rows.Count == 0 && dtLow.Rows.Count == 0)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("两边都不存在表{0}", currentTable);
                    compareResult.Rows.Add(currentTable, currentTableCn, "两边都不存在表", "", "", "", "", "");
                    Console.ForegroundColor = ConsoleColor.White;
                }
            }
            NPOIExcelRender.RenderToExcel(compareResult, string.Format(@"{0}.xls", "导出数据"));
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("数据差异分析完成，已成功导出Excel");
        }

        public static void WriteMsg(string msg)
        {
            string logPath = @"D:\Log.txt";
            using (var sw = new StreamWriter(logPath, true))
            {
                String logMsg = String.Format("[{0}]{1}", DateTime.Now, msg);
                sw.WriteLine(logMsg);
            }
        }
    }
}