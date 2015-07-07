using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.ComponentModel;

namespace DataInspection.Helper
{
    public class DataTableHelper
    {

        public enum CheckAction
        {
            [Description("添加")]
            Add,

            [Description("更新")]
            Update,

            [Description("删除")]
            Delete,

            [Description("相等")]
            Equal,
        }

        /// <summary>
        /// 比较两个DataTable数据
        /// </summary>
        /// <param name="dtSource">主库数据</param>
        /// <param name="dtSrc">目标库数据</param>
        /// <param name="dtDiff">对比结果</param>
        /// <param name="defaultKey">对比表主键</param>
        /// <param name="keyFields">对比表字段</param>
        public static void CompareTable(DataTable dtSource, DataTable dtSrc, out DataTable dtDiff, string defaultKey, List<string> keyFields)
        {
            //源记录集与目标记录集有一个为null则退出
            if (dtSource == null || dtSrc == null)
            {
                dtDiff = null;
                return;
            }

            //keyFields.ForEach(t => dtSource.Columns.Remove(t.ToString()));
            //keyFields.ForEach(t => dtSrc.Columns.Remove(t.ToString()));
            foreach (string t in keyFields)
            {
                if (dtSource.Columns.Contains(t.ToString()))
                {
                    dtSource.Columns.Remove(t.ToString());
                }
                if (dtSrc.Columns.Contains(t.ToString()))
                {
                    dtSrc.Columns.Remove(t.ToString());
                }
            }

            #region 排除表差异

            List<String> listSourceColumns = new List<string>();
            foreach (var sourceColumns in dtSource.Columns)
            {
                listSourceColumns.Add(sourceColumns.ToString());
            }

            List<String> listSrcColumns = new List<string>();
            foreach (var srcColumns in dtSrc.Columns)
            {
                listSrcColumns.Add(srcColumns.ToString());
            }

            var listDiffSource = listSourceColumns.Except(listSrcColumns);
            var listDiffSrc = listSrcColumns.Except(listSourceColumns);

            listDiffSource.ToList().ForEach(t => dtSource.Columns.Remove(t.ToString()));
            listDiffSrc.ToList().ForEach(t => dtSrc.Columns.Remove(t.ToString()));

            #endregion

            dtDiff = dtSource.Clone();
            object oSource;
            object oSrc;
            for (int i = 0; i < dtDiff.Columns.Count; i++)
            {
                dtSrc.Columns[dtDiff.Columns[i].ColumnName].SetOrdinal(i);
            }

            List<String> removeProps = new List<string>();

            //增加和修改的情况只需要检查主库
            foreach (DataRow item in dtSource.Rows)
            {

                string filterQuery = string.Empty;
                var keyArray = defaultKey.Split(',');

                if (keyArray.Length == 1 && keyArray[0] != string.Empty)
                {
                    filterQuery = string.Format("{0}='{1}'", defaultKey, item[0]);
                }
                else
                {
                    for (int i = 0; i < keyArray.Length; i++)
                    {
                        if (keyArray[i] != string.Empty)
                            filterQuery += string.Format("{0}='{1}' And ", keyArray[i], item[i]);
                    }
                    filterQuery += "1=1";
                }

                var hasRow = dtSrc.Select(filterQuery);

                CheckAction checkAction = CheckAction.Equal;
                if (hasRow.Count() > 0)
                {
                    //循环比对列值
                    foreach (var itemColumns in dtSource.Columns)
                    {
                        //获得列值
                        oSource = item[itemColumns.ToString()];
                        oSrc = hasRow[0][itemColumns.ToString()];

                        if (oSource != null && oSrc != null)
                        {
                            if (oSource.ToString() != oSrc.ToString())
                            {
                                checkAction = CheckAction.Update;
                                break;//某一列不同就跳出
                            }
                        }
                        else if (!oSource.Equals(oSrc))
                        {
                            checkAction = CheckAction.Update;
                            break;//某一列不同就跳出
                        }
                    }
                }
                else
                {
                    checkAction = CheckAction.Add;
                }

                //不相等的才写入差异
                if (!checkAction.Equals(CheckAction.Equal))
                {
                    item["CheckAction"] = checkAction.ToString();
                    dtDiff.Rows.Add(item.ItemArray);
                }
            }

            foreach (var removeProp in removeProps.Distinct())
            {
                dtDiff.Columns.Remove(removeProp);
            }

            //删除需要以子库为主
            foreach (DataRow item in dtSrc.Rows)
            {
                string filterQuery = string.Empty;
                var keyArray = defaultKey.Split(',');

                if (keyArray.Length == 1 && keyArray[0] != string.Empty)
                {
                    filterQuery = string.Format("{0}='{1}'", defaultKey, item[0]);
                }
                else
                {
                    for (int i = 0; i < keyArray.Length; i++)
                    {
                        if (keyArray[i] != string.Empty)
                            filterQuery += string.Format("{0}='{1}' And ", keyArray[i], item[i]);
                    }
                    filterQuery += "1=1";
                }

                var hasRow = dtSource.Select(filterQuery);

                if (hasRow.Count() < 1)
                {
                    item["CheckAction"] = CheckAction.Delete.ToString();
                    dtDiff.Rows.Add(item.ItemArray);
                }
            }
        }

        /// <summary>
        /// LINQ排除重复字段
        /// </summary>
        /// <typeparam name="TSource"></typeparam>
        /// <typeparam name="TKey"></typeparam>
        /// <param name="source"></param>
        /// <param name="keySelector"></param>
        /// <returns></returns>
        public static IEnumerable<TSource> DistinctBy<TSource, TKey>(IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            HashSet<TKey> seenKeys = new HashSet<TKey>();
            foreach (TSource element in source)
            {
                if (seenKeys.Add(keySelector(element)))
                {
                    yield return element;
                }
            }
        }

    }
}
