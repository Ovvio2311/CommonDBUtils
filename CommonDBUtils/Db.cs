using System.Data;
using System.Text.RegularExpressions;
using Dapper;
using Dapper.Contrib.Extensions;
using Microsoft.Extensions.Configuration;
using MySqlConnector;


namespace CommonDBUtils;

public class Db : DBConnection
{
    private static IConfiguration _configuration;

    //private string mongo_connstring;

    //private string mongo_datab;

    public Db(IConfiguration configuration)
    {
        _configuration = configuration;
        /*if (_configuration.GetSection("EnableEd25519AuthenticationPlugin") != null && Convert.ToBoolean(_configuration.GetSection("EnableEd25519AuthenticationPlugin").Value))
        {
            Ed25519AuthenticationPlugin.Install();
        }*/

        //mongo_connstring = _configuration["MongoDB:ConnectionString"];
        //mongo_datab = _configuration["MongoDB:Database"];
    }
    public static MySqlConnection getMySQLOpenConnection()
    {
        var connection = new MySqlConnection(_configuration["MySQL:ConnectionString"]);
        connection.Open();
        return connection;
    }
    public static MySqlConnection getMySQLOpenConnection(string connectionstring)
    {
        MySqlConnection mySqlConnection = new MySqlConnection(connectionstring);
        mySqlConnection.Open();
        return mySqlConnection;
    }

    private static bool IsStoredProcedureNameCorrect(string storedProcedureName)
    {
        if (string.IsNullOrEmpty(storedProcedureName))
        {
            return false;
        }

        if (storedProcedureName.StartsWith("[") && storedProcedureName.EndsWith("]"))
        {
            return Regex.IsMatch(storedProcedureName, "^[\\[]{1}[A-Za-z0-9_]+[\\]]{1}[\\.]{1}[\\[]{1}[A-Za-z0-9_]+[\\]]{1}$");
        }

        return Regex.IsMatch(storedProcedureName, "^[A-Za-z0-9]+[\\.]{1}[A-Za-z0-9]+$");
    }

    public long Execute<TModel>(string sql, TModel model, bool isStoredProcedure = false)
    {
        using IDbConnection dbConnection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            if (isStoredProcedure)
            {
                return dbConnection.Execute(sql, model, null, null, CommandType.StoredProcedure);
            }

            return dbConnection.Execute(sql, model, null, null, CommandType.Text);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(dbConnection);
        }
    }

    public async Task<long> ExecuteAsync<TModel>(string sql, TModel model, bool isStoredProcedure = false)
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        _ = 1;
        try
        {
            if (isStoredProcedure)
            {
                return await connection.ExecuteAsync(sql, model, null, null, CommandType.StoredProcedure);
            }

            return await connection.ExecuteAsync(sql, model, null, null, CommandType.Text);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public long Execute(string sql, DynamicParameters parameters, bool isStoredProcedure = false)
    {
        using IDbConnection dbConnection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            CommandType? commandType;
            if (isStoredProcedure)
            {
                commandType = CommandType.StoredProcedure;
                return dbConnection.Execute(sql, parameters, null, null, commandType);
            }

            commandType = CommandType.Text;
            return dbConnection.Execute(sql, parameters, null, null, commandType);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(dbConnection);
        }
    }

    public async Task<long> ExecuteAsync(string sql, DynamicParameters parameters, bool isStoredProcedure = false)
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        _ = 1;
        try
        {
            CommandType? commandType;
            if (isStoredProcedure)
            {
                commandType = CommandType.StoredProcedure;
                return await connection.ExecuteAsync(sql, parameters, null, null, commandType);
            }

            commandType = CommandType.Text;
            return await connection.ExecuteAsync(sql, parameters, null, null, commandType);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public async Task<IEnumerable<TModel>> QueryAsync<TModel>(string sql, DynamicParameters parameters = null)
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        _ = 1;
        try
        {
            CommandType? commandType;
            if (parameters != null)
            {
                commandType = CommandType.Text;
                return await connection.QueryAsync<TModel>(sql, parameters, null, null, commandType);
            }

            commandType = CommandType.Text;
            return await connection.QueryAsync<TModel>(sql, null, null, null, commandType);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public async Task<IEnumerable<TModel>> QueryAsync<TModel>(string sql, object parameters = null)
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        _ = 1;
        try
        {
            CommandType? commandType;
            if (parameters != null)
            {
                commandType = CommandType.Text;
                return await connection.QueryAsync<TModel>(sql, parameters, null, null, commandType);
            }

            commandType = CommandType.Text;
            return await connection.QueryAsync<TModel>(sql, null, null, null, commandType);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public IEnumerable<TModel> Query<TModel>(string sql, DynamicParameters parameters = null)
    {
        using IDbConnection dbConnection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            CommandType? commandType;
            if (parameters != null)
            {
                commandType = CommandType.Text;
                return dbConnection.Query<TModel>(sql, parameters, null, buffered: true, null, commandType);
            }

            commandType = CommandType.Text;
            return dbConnection.Query<TModel>(sql, null, null, buffered: true, null, commandType);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(dbConnection);
        }
    }

    public IEnumerable<TModel> Query<TModel>(string sql, object parameters = null)
    {
        using IDbConnection dbConnection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            CommandType? commandType;
            if (parameters != null)
            {
                commandType = CommandType.Text;
                return dbConnection.Query<TModel>(sql, parameters, null, buffered: true, null, commandType);
            }

            commandType = CommandType.Text;
            return dbConnection.Query<TModel>(sql, null, null, buffered: true, null, commandType);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(dbConnection);
        }
    }

    public async Task<long> ExecuteQueryAsync<TModel>(string sqlString, TModel model)
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            return await connection.ExecuteAsync(sqlString, model, null, null, CommandType.Text);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public async Task<long> ExecuteQueryAsync(string sqlString, DynamicParameters parameters = null)
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            return await connection.ExecuteAsync(sqlString, parameters, null, null, CommandType.Text);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public async Task<object> ExecuteScalarAsync<TModel>(string sql, TModel model)
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            return await connection.ExecuteScalarAsync(sql, model, null, null, CommandType.Text);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public async Task<object> ExecuteScalarAsync(string sql, DynamicParameters parameters = null)
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            return await connection.ExecuteScalarAsync(sql, parameters, null, null, CommandType.Text);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public IEnumerable<IDictionary<string, object>> ExecuteGenericSearch(string sql, dbPostContent inPara)
    {
        try
        {
            Dictionary<string, dynamic> dictionary = GlobalDBFunc.generateSQL(ref sql, inPara.valueDict, inPara.dataTypeArr);
            DynamicParameters dynamicParameters = new DynamicParameters();
            foreach (KeyValuePair<string, object> item in dictionary)
            {
                dynamicParameters.Add(item.Key, ((dynamic)item.Value).value, GlobalDBFunc.getDbTypeByStr(((dynamic)item.Value).dataType));
            }

            using IDbConnection cnn = new MySqlConnection(_configuration["MySQL:ConnectionString"]);
            return cnn.Query(sql, dynamicParameters, null, buffered: true, 180).Cast<IDictionary<string, object>>();
        }
        catch (Exception ex)
        {
            throw new Exception(ex.Message);
        }
    }

    public async Task<(IEnumerable<IDictionary<string, object>>, int totalcount)> ExecuteGenericSearchAsync(string sql, dbPostContent inPara)
    {
        _ = 1;
        try
        {
            new Dictionary<string, object>();
            int retTotalCnt = 0;
            string text = "";
            Dictionary<string, object> dictionary;
            if (inPara.isPaging)
            {
                (dictionary, text) = ((Dictionary<string, object>, string))GlobalDBFunc.generateSQL(ref sql, inPara.valueDict, inPara.dataTypeArr, inPara.isPaging, inPara.row_limit, inPara.row_offset, inPara.page_orderby);
            }
            else
            {
                dictionary = GlobalDBFunc.generateSQL(ref sql, inPara.valueDict, inPara.dataTypeArr);
            }

            DynamicParameters parameters = new DynamicParameters();
            foreach (KeyValuePair<string, object> item in dictionary)
            {
                parameters.Add(item.Key, ((srchPara)(dynamic)item.Value).value, GlobalDBFunc.getDbTypeByStr(((srchPara)(dynamic)item.Value).dataType));
            }

            if (inPara.isPaging && text != "")
            {
                using IDbConnection cnn = new MySqlConnection(_configuration["MySQL:ConnectionString"]);
                retTotalCnt = await cnn.QueryFirstAsync<int>(text, parameters, null, 180);
            }

            IEnumerable<IDictionary<string, object>> enumerable;
            using (IDbConnection cnn = new MySqlConnection(_configuration["MySQL:ConnectionString"]))
            {
                enumerable = (await cnn.QueryAsync(sql, parameters, null, 180)).Cast<IDictionary<string, object>>();
                if (!inPara.isPaging)
                {
                    retTotalCnt = enumerable.Count();
                }
            }

            return (enumerable, retTotalCnt);
        }
        catch (Exception ex)
        {
            throw new Exception(ex.Message);
        }
    }

    public async Task<(IEnumerable<IDictionary<string, object>>, int totalcount, string fieldlist, string criteria, string groupby, string orderby)> ExecuteEnhancedGenericSearchAsync(string sql, dbPostContent inPara)
    {
        _ = 1;
        try
        {
            new Dictionary<string, object>();
            int retTotalCnt = 0;
            string text = "";
            Dictionary<string, object> dictionary;
            string retFieldList;
            string retCriteria;
            string retGroupby;
            string retOrderby;
            if (!inPara.isPaging)
            {
                (dictionary, retFieldList, retCriteria, retGroupby, retOrderby) = ((Dictionary<string, object>, string, string, string, string))GlobalDBFunc.EnhancedGenerateSQL(ref sql, inPara.valueDict, inPara.dataTypeArr);
            }
            else
            {
                (dictionary, text, retFieldList, retCriteria, retGroupby, retOrderby) = ((Dictionary<string, object>, string, string, string, string, string))GlobalDBFunc.EnhancedGenerateSQL(ref sql, inPara.valueDict, inPara.dataTypeArr, inPara.isPaging, inPara.row_limit, inPara.row_offset, inPara.page_orderby);
            }

            DynamicParameters parameters = new DynamicParameters();
            foreach (KeyValuePair<string, object> item in dictionary)
            {
                parameters.Add(item.Key, ((srchPara)(dynamic)item.Value).value, GlobalDBFunc.getDbTypeByStr(((srchPara)(dynamic)item.Value).dataType));
            }

            if (inPara.isPaging && text != "")
            {
                using IDbConnection cnn = new MySqlConnection(_configuration["MySQL:ConnectionString"]);
                retTotalCnt = await cnn.QueryFirstAsync<int>(text, parameters, null, 180);
            }

            IEnumerable<IDictionary<string, object>> enumerable;
            using (IDbConnection cnn = new MySqlConnection(_configuration["MySQL:ConnectionString"]))
            {
                enumerable = (await cnn.QueryAsync(sql, parameters, null, 180)).Cast<IDictionary<string, object>>();
                if (!inPara.isPaging)
                {
                    retTotalCnt = enumerable.Count();
                }
            }

            return (enumerable, retTotalCnt, retFieldList, retCriteria, retGroupby, retOrderby);
        }
        catch (Exception ex)
        {
            throw new Exception(ex.Message);
        }
    }

    //public async Task<(IEnumerable<IDictionary<string, object>>, int totalcount)> ExecuteMongoGenericSearchAsync(string collection, List<string[]> sortFields, dbPostContent inPara)
    //{
    //    _ = 5;
    //    try
    //    {
    //        FilterDefinition<object> searchFilter = FilterDefinition<object>.Empty;
    //        for (int i = 0; i < inPara.valueDict.Count; i++)
    //        {
    //            string key = inPara.valueDict.ElementAt(i).Key;
    //            string value = inPara.valueDict.ElementAt(i).Value;
    //            string text = inPara.dataTypeArr[i];
    //            if (string.IsNullOrEmpty(value))
    //            {
    //                continue;
    //            }

    //            if (InternalUtils.isStrArr(value, out var strArr))
    //            {
    //                if (strArr[0].Substring(0, 3) == "dr_")
    //                {
    //                    DateTime value2 = DateTime.Now;
    //                    DateTime value3 = DateTime.Now;
    //                    for (int j = 0; j < strArr.Length; j++)
    //                    {
    //                        if (j == 0)
    //                        {
    //                            strArr[j] = strArr[j].Substring(3);
    //                            value2 = DateTime.Parse(strArr[j], CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
    //                        }
    //                        else
    //                        {
    //                            value3 = DateTime.Parse(strArr[j], CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).AddDays(1.0);
    //                        }
    //                    }

    //                    searchFilter &= Builders<object>.Filter.Gte(key, value2) & Builders<object>.Filter.Lt(key, value3);
    //                    continue;
    //                }

    //                List<FilterDefinition<object>> list = new List<FilterDefinition<object>>();
    //                (bool, bool, bool) tuple = GlobalDBFunc.isLikeMatch(value);
    //                bool item = tuple.Item1;
    //                bool item2 = tuple.Item2;
    //                bool item3 = tuple.Item3;
    //                for (int k = 0; k < strArr.Length; k++)
    //                {
    //                    string text2 = strArr[k].Replace("%", "");
    //                    if (k == 0)
    //                    {
    //                        if (item)
    //                        {
    //                            BsonRegularExpression regex = item2 ? new BsonRegularExpression(new Regex("^" + text2 + ".*", RegexOptions.IgnoreCase)) : !item3 ? new BsonRegularExpression(new Regex(text2, RegexOptions.IgnoreCase)) : new BsonRegularExpression(new Regex(text2 + "$", RegexOptions.IgnoreCase));
    //                            list.Add(Builders<object>.Filter.Regex(key, regex));
    //                        }
    //                        else
    //                        {
    //                            list.Add(Builders<object>.Filter.Regex(key, text2));
    //                        }
    //                    }
    //                    else if (k == strArr.Length - 1)
    //                    {
    //                        if (item)
    //                        {
    //                            BsonRegularExpression regex2 = item2 ? new BsonRegularExpression(new Regex("^" + text2 + ".*", RegexOptions.IgnoreCase)) : !item3 ? new BsonRegularExpression(new Regex(text2, RegexOptions.IgnoreCase)) : new BsonRegularExpression(new Regex(text2 + "$", RegexOptions.IgnoreCase));
    //                            list.Add(Builders<object>.Filter.Regex(key, regex2));
    //                        }
    //                        else
    //                        {
    //                            list.Add(Builders<object>.Filter.Regex(key, text2));
    //                        }
    //                    }
    //                    else if (item)
    //                    {
    //                        BsonRegularExpression regex3 = item2 ? new BsonRegularExpression(new Regex("^" + text2 + ".*", RegexOptions.IgnoreCase)) : !item3 ? new BsonRegularExpression(new Regex(text2, RegexOptions.IgnoreCase)) : new BsonRegularExpression(new Regex(text2 + "$", RegexOptions.IgnoreCase));
    //                        list.Add(Builders<object>.Filter.Regex(key, regex3));
    //                    }
    //                    else
    //                    {
    //                        list.Add(Builders<object>.Filter.Regex(key, text2));
    //                    }

    //                    searchFilter &= Builders<object>.Filter.Or(list);
    //                }
    //            }
    //            else
    //            {
    //                var (item, item2, item3) = GlobalDBFunc.isLikeMatch(value);
    //                if (item)
    //                {
    //                    value = value.Replace("%", "");
    //                    BsonRegularExpression regex4 = item2 ? new BsonRegularExpression(new Regex("^" + value + ".*", RegexOptions.IgnoreCase)) : !item3 ? new BsonRegularExpression(new Regex(value, RegexOptions.IgnoreCase)) : new BsonRegularExpression(new Regex(value + "$", RegexOptions.IgnoreCase));
    //                    searchFilter &= Builders<object>.Filter.Regex(key, regex4);
    //                }
    //                else if (text == "datetime")
    //                {
    //                    DateTime value4 = DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
    //                    DateTime value5 = DateTime.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal).AddDays(1.0);
    //                    searchFilter &= Builders<object>.Filter.Gte(key, value4) & Builders<object>.Filter.Lt(key, value5);
    //                }
    //                else
    //                {
    //                    searchFilter &= Builders<object>.Filter.Eq(key, value);
    //                }
    //            }
    //        }

    //        MongoDbContext _dbContext = new MongoDbContext(mongo_connstring, mongo_datab);
    //        int retTotalCnt = 0;
    //        new List<object>();
    //        List<SortDefinition<object>> list2 = new List<SortDefinition<object>>();
    //        if (sortFields != null && sortFields.Count > 0)
    //        {
    //            foreach (string[] sortField in sortFields)
    //            {
    //                string text3 = sortField[0];
    //                if (sortField[1].ToLower().Trim() == "asc")
    //                {
    //                    list2.Add(Builders<object>.Sort.Ascending(text3));
    //                }
    //                else
    //                {
    //                    list2.Add(Builders<object>.Sort.Descending(text3));
    //                }
    //            }
    //        }

    //        List<object> res;
    //        if (inPara.isPaging && inPara.row_limit > -1 && inPara.row_offset > -1)
    //        {
    //            if (inPara.page_orderby != null)
    //            {
    //                int num = 1;
    //                string text4 = "";
    //                List<SortDefinition<object>> list3 = new List<SortDefinition<object>>();
    //                string[] page_orderby = inPara.page_orderby;
    //                foreach (string text5 in page_orderby)
    //                {
    //                    if (num % 2 == 0)
    //                    {
    //                        text4 = text5;
    //                    }
    //                    else if (text5.ToLower().Trim() == "asc")
    //                    {
    //                        list3.Add(Builders<object>.Sort.Ascending(text4));
    //                    }
    //                    else
    //                    {
    //                        list3.Add(Builders<object>.Sort.Descending(text4));
    //                    }

    //                    num++;
    //                }

    //                SortDefinition<object> sort = Builders<object>.Sort.Combine(list3);
    //                res = await _dbContext.DynamicCollection(collection).Find<object>(searchFilter).Sort(sort)
    //                    .Skip(inPara.row_offset)
    //                    .Limit(inPara.row_limit)
    //                    .ToListAsync();
    //            }
    //            else if (sortFields != null && sortFields.Count > 0)
    //            {
    //                SortDefinition<object> sort = Builders<object>.Sort.Combine(list2);
    //                res = await _dbContext.DynamicCollection(collection).Find<object>(searchFilter).Sort(sort)
    //                    .Skip(inPara.row_offset)
    //                    .Limit(inPara.row_limit)
    //                    .ToListAsync();
    //            }
    //            else
    //            {
    //                res = await _dbContext.DynamicCollection(collection).Find<object>(searchFilter).Skip(inPara.row_offset)
    //                    .Limit(inPara.row_limit)
    //                    .ToListAsync();
    //            }

    //            retTotalCnt = Convert.ToInt32(await _dbContext.DynamicCollection(collection).Find<object>(searchFilter).CountDocumentsAsync());
    //        }
    //        else if (sortFields != null && sortFields.Count > 0)
    //        {
    //            SortDefinition<object> sort = Builders<object>.Sort.Combine(list2);
    //            res = await _dbContext.DynamicCollection(collection).Find<object>(searchFilter).Sort(sort)
    //                .ToListAsync();
    //        }
    //        else
    //        {
    //            res = await _dbContext.DynamicCollection(collection).Find<object>(searchFilter).ToListAsync();
    //        }

    //        IEnumerable<IDictionary<string, object>> enumerable = res.AsQueryable().Cast<IDictionary<string, object>>();
    //        if (!inPara.isPaging)
    //        {
    //            retTotalCnt = enumerable.Count();
    //        }

    //        return (enumerable, retTotalCnt);
    //    }
    //    catch
    //    {
    //        throw;
    //    }
    //}

    public async Task<bool> InsertAsync<TModel>(TModel model, IDbTransaction tran = null) where TModel : class
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            SqlMapperExtensions.TableNameMapper = (tModel) => tModel.Name;
            if (await connection.InsertAsync(new List<TModel> { model }, tran) == 1)
            {
                return true;
            }

            return false;
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public async Task<int> InsertAsync<TModel>(IEnumerable<TModel> modelList, IDbTransaction tran = null) where TModel : class
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            SqlMapperExtensions.TableNameMapper = (tModel) => tModel.Name;
            return await connection.InsertAsync(modelList, tran);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public async Task<bool> UpdateAsync<TModel>(TModel model, IDbTransaction tran = null) where TModel : class
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            SqlMapperExtensions.TableNameMapper = (tModel) => tModel.Name;
            return await connection.UpdateAsync(model, tran);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

    public async Task<bool> UpdateAsync<TModel>(IEnumerable<TModel> model, IDbTransaction tran = null) where TModel : class
    {
        using IDbConnection connection = getConnection(_configuration["MySQL:ConnectionString"]);
        try
        {
            SqlMapperExtensions.TableNameMapper = (tModel) => tModel.Name;
            return await connection.UpdateAsync(model, tran);
        }
        catch (Exception)
        {
            throw;
        }
        finally
        {
            CloseConnection(connection);
        }
    }

}

