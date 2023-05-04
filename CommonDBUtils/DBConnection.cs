using System.Data;
using System.Data.Common;
using MySqlConnector;

namespace CommonDBUtils
{

    public class DBConnection : IDisposable
    {
        protected static IDbConnection getConnection(string connectionstring)
        {
            IDbConnection dbConnection = OpenConnection(new MySqlConnectionStringBuilder(connectionstring));
            dbConnection.Open();
            return dbConnection;
        }

        private static IDbConnection OpenConnection(DbConnectionStringBuilder connectionString)
        {
            return new MySqlConnection(connectionString.ConnectionString);
        }

        protected static bool CloseConnection(IDbConnection connection)
        {
            if (connection.State != 0)
            {
                connection.Close();
            }

            return true;
        }

        private static void ClearPool()
        {
            MySqlConnection.ClearAllPools();
        }

        public void Dispose()
        {
            ClearPool();
        }
    }
    public class dbPostContent
    {
        public Dictionary<string, string> valueDict { get; set; }

        public string[] dataTypeArr { get; set; }

        public bool isPaging { get; set; }

        public string[] page_orderby { get; set; }

        public int row_offset { get; set; }

        public int row_limit { get; set; }
    }
    public class GlobalDBFunc
    {
        public static Dictionary<string, dynamic> generateSQL(ref string sql, Dictionary<string, string> valueDict, string[] dataTypeArr)
        {
            new Dictionary<string, object>();
            return ((ValueTuple<Dictionary<string, object>, string, string, string, string, string>)fn_generateSQL(ref sql, valueDict, dataTypeArr)).Item1;
        }

        public static (Dictionary<string, dynamic>, string totalCntSQL) generateSQL(ref string sql, Dictionary<string, string> valueDict, string[] dataTypeArr, bool isPaging = false, int p_limit = -1, int p_offset = -1, string[] p_orderby = null)
        {
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            string text = "";
            (dictionary, text, _, _, _, _) = ((Dictionary<string, object>, string, string, string, string, string))fn_generateSQL(ref sql, valueDict, dataTypeArr, isPaging, p_limit, p_offset, p_orderby);
            return (dictionary, text);
        }

        public static (Dictionary<string, dynamic>, string fieldlist, string criteria, string groupby, string orderby) EnhancedGenerateSQL(ref string sql, Dictionary<string, string> valueDict, string[] dataTypeArr)
        {
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            string text = "";
            string text2 = "";
            string text3 = "";
            string text4 = "";
            (dictionary, _, text, text2, text3, text4) = ((Dictionary<string, object>, string, string, string, string, string))fn_generateSQL(ref sql, valueDict, dataTypeArr);
            return (dictionary, text, text2, text3, text4);
        }

        public static (Dictionary<string, dynamic>, string totalCntSQL, string fieldlist, string criteria, string groupby, string orderby) EnhancedGenerateSQL(ref string sql, Dictionary<string, string> valueDict, string[] dataTypeArr, bool isPaging = false, int p_limit = -1, int p_offset = -1, string[] p_orderby = null)
        {
            return fn_generateSQL(ref sql, valueDict, dataTypeArr, isPaging, p_limit, p_offset, p_orderby);
        }

        public static (Dictionary<string, dynamic>, string totalCntSQL, string fieldlist, string criteria, string groupby, string orderby) fn_generateSQL(ref string sql, Dictionary<string, string> valueDict, string[] dataTypeArr, bool isPaging = false, int p_limit = -1, int p_offset = -1, string[] p_orderby = null)
        {
            string text = sql;
            string text2 = "";
            string text3 = "";
            string text4 = "";
            string text5 = "";
            string item = "";
            string text6 = "";
            string text7 = "";
            string text8 = "";
            string text9 = "";
            string item2 = "";
            List<string> list = new List<string>();
            if (sql.ToUpper().Contains("WHERE 2=2") || sql.ToUpper().Contains("WHERE 2 = 2"))
            {
                string text10 = sql;
                while (text10.ToUpper().Contains("WHERE 2 = 2") || text10.ToUpper().Contains("WHERE 2 = 2"))
                {
                    if (text10.ToUpper().Contains("WHERE 2=2"))
                    {
                        list.Add(text10.Substring(0, text10.ToUpper().IndexOf("WHERE 2=2") - 1).Trim());
                        text10 = text10.Replace(text10.Substring(0, text10.ToUpper().IndexOf("WHERE 2 = 2") + 9), "");
                    }
                    else if (text10.ToUpper().Contains("WHERE 2 = 2"))
                    {
                        list.Add(text10.Substring(0, text10.ToUpper().IndexOf("WHERE 2 = 2") - 1).Trim());
                        text10 = text10.Replace(text10.Substring(0, text10.ToUpper().IndexOf("WHERE 2 = 2") + 11), "");
                    }
                }

                if (text10.Contains("WHERE"))
                {
                    text2 = text10.Substring(text10.ToUpper().IndexOf("WHERE") + 5, text10.Length - text10.ToUpper().IndexOf("WHERE") - 5).Trim();
                    text = text10.Substring(0, text10.ToUpper().IndexOf("WHERE") - 1).Trim();
                }
                else
                {
                    text = text10;
                }
            }
            else if (sql.ToUpper().Contains("WHERE"))
            {
                text2 = sql.Substring(sql.ToUpper().IndexOf("WHERE") + 5, sql.Length - sql.ToUpper().IndexOf("WHERE") - 5).Trim();
                text = sql.Substring(0, sql.ToUpper().IndexOf("WHERE") - 1).Trim();
            }
            else if (!sql.ToUpper().Contains("WHERE"))
            {
                text2 = "";
                text = sql;
            }

            if (text2 != "" && (text2.ToUpper().Contains("GROUP BY") || text2.ToUpper().Contains("ORDER BY")))
            {
                if (text2.ToUpper().Contains("GROUP BY"))
                {
                    text3 = " group by " + text2.Substring(text2.ToUpper().LastIndexOf("GROUP BY") + 8, text2.Length - text2.ToUpper().LastIndexOf("GROUP BY") - 8).Trim();
                    if (text3.ToUpper().Contains("ORDER BY"))
                    {
                        text3 = text3.Substring(0, text3.ToUpper().LastIndexOf("ORDER BY") - 1);
                    }
                }

                if (text2.ToUpper().Contains("ORDER BY"))
                {
                    text4 = " order by " + text2.Substring(text2.ToUpper().LastIndexOf("ORDER BY") + 8, text2.Length - text2.ToUpper().LastIndexOf("ORDER BY") - 8).Trim();
                }

                text2 = !text2.ToUpper().Contains("GROUP BY") ? text2.Substring(0, text2.ToUpper().LastIndexOf("ORDER BY") - 1).Trim() : text2.Substring(0, text2.ToUpper().LastIndexOf("GROUP BY") - 1).Trim();
            }
            else if (text.ToUpper().Contains("GROUP BY") || text.ToUpper().Contains("ORDER BY"))
            {
                if (text.ToUpper().Contains("GROUP BY"))
                {
                    text3 = " group by " + text.Substring(text.ToUpper().LastIndexOf("GROUP BY") + 8, text.Length - text.ToUpper().LastIndexOf("GROUP BY") - 8).Trim();
                    if (text3.ToUpper().Contains("ORDER BY"))
                    {
                        text3 = text3.Substring(0, text3.ToUpper().LastIndexOf("ORDER BY") - 1);
                    }
                }

                if (text.ToUpper().Contains("ORDER BY"))
                {
                    text4 = " order by " + text.Substring(text.ToUpper().LastIndexOf("ORDER BY") + 8, text.Length - text.ToUpper().LastIndexOf("ORDER BY") - 8).Trim();
                }

                text = !text.ToUpper().Contains("GROUP BY") ? text.Substring(0, text.ToUpper().LastIndexOf("ORDER BY") - 1).Trim() : text.Substring(0, text.ToUpper().LastIndexOf("GROUP BY") - 1).Trim();
            }

            string text11 = "";
            Dictionary<string, object> dictionary = new Dictionary<string, object>();
            if (valueDict != null)
            {
                for (int i = 0; i < valueDict.Count; i++)
                {
                    string key = valueDict.ElementAt(i).Key;
                    string value = valueDict.ElementAt(i).Value;
                    if (string.IsNullOrEmpty(value))
                    {
                        continue;
                    }

                    text11 += " and ";
                    if (InternalUtils.isStrArr(value, out var strArr))
                    {
                        if (strArr[0].Substring(0, 3) == "dr_")
                        {
                            for (int j = 0; j < strArr.Length; j++)
                            {
                                string text12 = key + j;
                                if (j == 0)
                                {
                                    strArr[j] = strArr[j].Substring(3);
                                    text11 = text11 + " " + key + " >= @" + text12;
                                }
                                else
                                {
                                    text11 = text11 + " and " + key + " < date_add(@" + text12 + ", interval 1 day)";
                                }

                                dictionary.Add(text12, new srchPara
                                {
                                    value = strArr[j],
                                    dataType = dataTypeArr[i]
                                });
                            }

                            continue;
                        }

                        if (strArr[0].Substring(0, 4) == "dtr_")
                        {
                            for (int k = 0; k < strArr.Length; k++)
                            {
                                string text13 = key + k;
                                if (k == 0)
                                {
                                    strArr[k] = strArr[k].Substring(4);
                                    text11 = text11 + " " + key + " >= @" + text13;
                                }
                                else
                                {
                                    text11 = text11 + " and " + key + " <= @" + text13;
                                }

                                dictionary.Add(text13, new srchPara
                                {
                                    value = strArr[k],
                                    dataType = dataTypeArr[i]
                                });
                            }

                            continue;
                        }

                        for (int l = 0; l < strArr.Length; l++)
                        {
                            string text14 = key + l;
                            text11 = l != 0 ? l != strArr.Length - 1 ? text11 + " or " + key + getOperator(value) + text14 : text11 + " or " + key + getOperator(value) + text14 + ")" : text11 + "(" + key + getOperator(value) + text14;
                            dictionary.Add(text14, new srchPara
                            {
                                value = strArr[l],
                                dataType = dataTypeArr[i]
                            });
                        }
                    }
                    else
                    {
                        text11 = text11 + key + getOperator(value) + key;
                        dictionary.Add(key, new srchPara
                        {
                            value = value,
                            dataType = dataTypeArr[i]
                        });
                    }
                }
            }

            if (isPaging && p_limit > -1 && p_offset > -1)
            {
                text5 = $"limit {p_limit} offset {p_offset}";
                if (p_orderby != null)
                {
                    text4 = " order by";
                    int num = 1;
                    string text15 = "";
                    foreach (string text16 in p_orderby)
                    {
                        text15 = num % 2 != 0 ? text15 + (text15 != "" ? "," : "") + " " + text16 : text15 + " " + text16;
                        num++;
                    }

                    text4 = text4 + " " + text15;
                }
            }

            text7 = " where 1 = 1 " + text11.Trim();
            text8 = text3;
            text9 = text4;
            if (list.Count > 0)
            {
                sql = "";
                foreach (string item3 in list)
                {
                    sql = sql + (sql != "" ? " " : "") + item3 + text7;
                }

                sql += text;
                if (text2 != "")
                {
                    sql = sql + (text2.Length > 3 && text2.Trim().Substring(0, 4).ToUpper() == "AND " ? " where 1 = 1 " : " where ") + text2;
                    text7 = text7 + (text2.Length > 3 && text2.Trim().Substring(0, 4).ToUpper() == "AND " ? " where 1 = 1 " : " where ") + text2;
                }

                text = sql;
                sql = sql + text3 + text4;
                sql = sql.Trim();
            }
            else
            {
                sql = text + text7;
                if (text2 != "")
                {
                    sql = sql + (text2.Length > 3 && text2.Trim().Substring(0, 4).ToUpper() == "AND " ? " " : " and ") + text2;
                    text7 = text7 + (text2.Length > 3 && text2.Trim().Substring(0, 4).ToUpper() == "AND " ? " " : " and ") + text2;
                }

                text = sql;
                sql = sql + text3 + text4;
                sql = sql.Trim();
            }

            text6 = text;
            if (text6.ToUpper().Contains("SELECT"))
            {
                text6 = text6.Substring(text6.ToUpper().IndexOf("SELECT") + 6, text6.Length - text6.ToUpper().IndexOf("SELECT") - 6).Trim();
            }

            if (text6.ToUpper().Contains("DISTINCT"))
            {
                text6 = text6.Substring(text6.ToUpper().IndexOf("DISTINCT") + 8, text6.Length - text6.ToUpper().IndexOf("DISTINCT") - 8).Trim();
            }

            if (text6.ToUpper().Contains("FROM"))
            {
                text6 = text6.Substring(0, text6.ToUpper().IndexOf("FROM") - 1).Trim();
            }

            if (text6.Trim() != "")
            {
                text6 = text6.Replace("\\r", "", StringComparison.CurrentCultureIgnoreCase).Replace("\\n", "", StringComparison.CurrentCultureIgnoreCase).Trim()
                    .Replace("  ", " ")
                    .Replace(", ", ",");
                item2 = text6;
                item = "select count(*) " + text.Substring(text.ToUpper().IndexOf("FROM"), text.Length - text.ToUpper().IndexOf("FROM")).Trim();
            }

            if (isPaging && p_limit > -1 && p_offset > -1)
            {
                sql = sql + " " + text5;
            }

            return (dictionary, item, item2, text7, text8, text9);
        }

        private static string getOperator(string y)
        {
            string text = "";
            if (y.Length > 2)
            {
                if (y.Substring(0, 1) == "%" && y.Substring(y.Length - 1, 1) == "%")
                {
                    return " like @";
                }

                if (y.Substring(y.Length - 1, 1) == "%" || y.Substring(0, 1) == "%")
                {
                    return " like @";
                }

                return " = @";
            }

            return " = @";
        }

        public static (bool, bool, bool) isLikeMatch(string y)
        {
            if (y.Length > 2)
            {
                if (y.Substring(0, 1) == "%" && y.Substring(y.Length - 1, 1) == "%")
                {
                    return (true, false, false);
                }

                if (y.Substring(y.Length - 1, 1) == "%")
                {
                    return (true, true, false);
                }

                if (y.Substring(0, 1) == "%")
                {
                    return (true, false, true);
                }

                return (false, false, false);
            }

            return (false, false, true);
        }

        private static bool isDateRange(string y, out string[] dateStrArr)
        {
            bool result = false;
            dateStrArr = new string[0];
            try
            {
                if (y.Substring(0, 3) == "dr_")
                {
                    y = y.Substring(0, 3);
                    dateStrArr = y.Split(",", StringSplitOptions.RemoveEmptyEntries);
                    if (dateStrArr.Length > 1)
                    {
                        return true;
                    }

                    return result;
                }

                return result;
            }
            catch
            {
                return false;
            }
        }

        public static DbType getDbTypeByStr(string inVal)
        {
            if (!string.IsNullOrEmpty(inVal))
            {
                return inVal.ToLower() switch
                {
                    "binary" => DbType.Binary,
                    "boolean" => DbType.Boolean,
                    "date" => DbType.Date,
                    "datetime" => DbType.DateTime,
                    "decimal" => DbType.Decimal,
                    "double" => DbType.Double,
                    "int32" => DbType.Int32,
                    _ => DbType.String,
                };
            }

            return DbType.String;
        }
    }

    public class srchPara
    {
        public string value { get; set; }

        public string dataType { get; set; }
    }
    internal class InternalUtils
    {
        public static bool isStrArr(string y, out string[] strArr)
        {
            bool result = false;
            strArr = new string[0];
            try
            {
                strArr = y.Split(",", StringSplitOptions.RemoveEmptyEntries);
                if (strArr.Length > 1)
                {
                    return true;
                }

                return result;
            }
            catch
            {
                return false;
            }
        }
    }


}
