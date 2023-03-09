using Dapper;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using System.Transactions;


namespace Dapper.Generic
{
    public class DapperGenericService
    {
        public DbContext context { get; set; }
        public string Connectionstring { get; set; }
        public static class FixedTexts
        {
            public static GeneralSqlQuery SqlQuery { get { return new GeneralSqlQuery(); } private set { } }
        }

        public class GeneralSqlQuery
        {
            public string GetAll(string tableName) => $"SELECT * FROM {tableName}";
            public string GetFirst(string tableName) => $"SELECT Top(1) * FROM {tableName}";

        }

        private IDbConnection _db;
        public DapperGenericService(DbContext dbContext, string connectionString)
        {
            context = dbContext;
            Connectionstring = connectionString;
            _db = new SqlConnection(Connectionstring);
        }

        public int Execute(string sql, object data)
        {
            return _db.Execute(sql, data);
        }
        public async Task ExecuteAsync(string sql, object data)
        {
            await _db.ExecuteAsync(sql, data);
        }


        public T ExecuteTransactionScope<T>(Func<T> func)
        {
            try
            {
                using (var transaction = new TransactionScope())
                {
                    var r = func();
                    transaction.Complete();
                    return r;
                }
            }
            catch (Exception ex) { throw ex; }
        }
        public async Task<T> ExecuteTransactionScopeAsync<T>(Func<Task<T>> func)
        {
            try
            {
                using (var transaction = new TransactionScope())
                {
                    var r = await func();
                    transaction.Complete();
                    return r;
                }
            }
            catch (Exception ex) { throw ex; }
        }
        public async Task ExecuteTransactionScopeAsync(Func<Task> func)
        {
            try
            {
                using (var transaction = new TransactionScope())
                {
                    await func();
                    transaction.Complete();
                }
            }
            catch (Exception ex) { throw ex; }
        }


        public T QueryFirstOrDefault<T>(string sql)
        {
            return _db.QueryFirstOrDefault<T>(sql);
        }
        public async Task<T> QueryFirstOrDefaultAsync<T>(string sql)
        {
            return await _db.QueryFirstOrDefaultAsync<T>(sql);
        }


        public async Task<T> QueryFirstOrDefaultAsync<T>()
        {
            return await _db.QueryFirstOrDefaultAsync<T>(FixedTexts.SqlQuery.GetFirst(GetTableName<T>()));
        }
        public T QueryFirstOrDefault<T>()
        {
            return _db.QueryFirstOrDefault<T>(FixedTexts.SqlQuery.GetFirst(GetTableName<T>()));
        }


        public IEnumerable<T> Query<T>(string sql = "")
        {
            if (string.IsNullOrEmpty(sql))
                sql = FixedTexts.SqlQuery.GetAll(GetTableName<T>());
            return _db.Query<T>(sql);
        }
        public async Task<IEnumerable<T>> QueryAsync<T>(string sql = "")
        {
            if (string.IsNullOrEmpty(sql))
                sql = FixedTexts.SqlQuery.GetAll(GetTableName<T>());
            return await _db.QueryAsync<T>(sql);
        }


        public int Insert<T>(T entity)
        {
            var columns = GetColumns<T>(entity);
            var stringOfColumns = string.Join(", ", columns);
            var stringOfParameters = string.Join(", ", columns.Select(e => "@" + e));
            var query = $"insert into {GetTableName<T>()} ({stringOfColumns}) values ({stringOfParameters})";

            return Execute(query, entity);
        }
        public int Update<T>(T entity)
        {
            var keyTable = GetKey<T>(entity);
            var columns = GetColumns<T>(entity).Where(a => a != keyTable.Name);
            var stringOfColumns = string.Join(", ", columns.Select(e => $"{e} = @{e}"));
            var query = $"update {GetTableName<T>()} set {stringOfColumns} where {keyTable.Name} = @{keyTable.Name}";

            return Execute(query, entity);
        }
        public int Delete<T>(T entity)
        {
            var keyTable = GetKey<T>(entity);
            var query = $"delete from {GetTableName<T>()} where {keyTable.Name} = @{keyTable.Name}";

            return Execute(query, entity);
        }

        private PropertyInfo GetKey<T>(T entity)
        {
            var keyName = context.Model.FindEntityType(typeof(T)).FindPrimaryKey().Properties
                .Select(x => x.Name).Single();

            return entity.GetType().GetProperty(keyName);
        }
        private string GetTableName<T>()
        {
            var typeName = context.Model.FindEntityType(typeof(T));
            var schemaName = typeName.GetSchema();
            var tableName = (string.IsNullOrEmpty(schemaName) ? "" : $"[{schemaName}]") + $"[{typeof(T).Name}]";
            return tableName;
        }
        private IEnumerable<string> GetColumns<T>(T Entity)
        {
            List<string> entityColumn = new List<string>();
            System.Reflection.PropertyInfo[] oProperty = Entity.GetType().GetProperties();
            Type myType = Entity.GetType();
            IList<PropertyInfo> props = new List<PropertyInfo>(myType.GetProperties());
            foreach (var prop in props)
            {
                var ItemName = prop.Name;
                var ItemValue = prop.GetValue(Entity, null);
                var propMethod = prop.GetGetMethod();
                if (ItemValue != null && !propMethod.IsCollectible && !propMethod.IsVirtual && !propMethod.IsConstructor)
                {
                    entityColumn.Add(ItemName);
                }
            }
            return entityColumn;
        }


    }
}