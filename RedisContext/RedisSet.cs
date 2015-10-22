namespace RedisContext
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using StackExchange.Redis;

    public sealed class RedisSet<T> where T : RedisEntity
    {
        public string Name { get; private set; }

        public RedisContext Context { get; private set; }

        public RedisSet(RedisContext ctx, string name)
        {
            Context = ctx;
            Name = name;
        }

        public T Fetch(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(id);
            var serialized = db.StringGet(key);
            return Convert(serialized);
        }

        public async Task<T> FetchAsync(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(id);
            var serialized = await db.StringGetAsync(key);
            return Convert(serialized);
        }

        public IEnumerable<T> Fetch(IEnumerable<string> ids)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var keys = ids.Select(GetKey);
            var serialized = db.StringGet(keys.ToArray());
            return serialized.Select(Convert);
        }

        public async Task<IEnumerable<T>> FetchAsync(IEnumerable<string> ids)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var keys = ids.Select(GetKey);
            var serialized = await db.StringGetAsync(keys.ToArray());
            return serialized.Select(Convert);
        }

        public IEnumerable<T> Fetch(string id, int limit, int offset = 0)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var index = GetIndexKey();

            var keys = db.SortedSetRangeByValue(index, id, default(RedisValue), Exclude.None, offset, limit).Select(x => x.ToString());
            return Fetch(keys);
        }

        public Task<IEnumerable<T>> FetchAsync(string id, int limit, int offset = 0)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var index = GetIndexKey();

            var keys = db.SortedSetRangeByValue(index, id, default(RedisValue), Exclude.None, offset, limit).Select(x => x.ToString());
            return FetchAsync(keys);
        }

        public IEnumerable<T> Fetch(string min, string max)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var index = GetIndexKey();

            var keys = db.SortedSetRangeByValue(index, min, max).Select(x => x.ToString());
            return Fetch(keys);
        }

        public Task<IEnumerable<T>> FetchAsync(string min, string max)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var index = GetIndexKey();

            var keys = db.SortedSetRangeByValue(index, min, max).Select(x => x.ToString());
            return FetchAsync(keys);
        }

        public bool Insert(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = Convert(entity);

            var tran = db.CreateTransaction();
            tran.AddCondition(Condition.KeyNotExists(key));

            tran.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", entity.Version)
            });

            var success = tran.Execute();

            if (success)
            {
                UpdateIndex(entity);
            }

            return success;
        }

        public async Task<bool> InsertAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = Convert(entity);

            var tran = db.CreateTransaction();
            tran.AddCondition(Condition.KeyNotExists(key));

            tran.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", _version)
            });

            var success = await tran.ExecuteAsync();

            if (success)
            {
                await UpdateIndexAsync(entity);
            }

            return success;
        }

        public void InsertOrReplace(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = Convert(entity);

            db.HashSet(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", _version)
            });

            UpdateIndex(entity);
        }

        public async Task InsertOrReplaceAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = Convert(entity);

            await db.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", _version)
            });

            await UpdateIndexAsync(entity);
        }

        public bool Update(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            var etag = entity.Etag;
            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = Convert(entity);

            var tran = db.CreateTransaction();
            tran.AddCondition(Condition.HashEqual(key, "etag", etag));

            tran.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", _version)
            });

            return tran.Execute();
        }

        public Task<bool> UpdateAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            var etag = entity.Etag;
            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = Convert(entity);

            var tran = db.CreateTransaction();
            tran.AddCondition(Condition.HashEqual(key, "etag", etag));

            tran.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", _version)
            });

            return tran.ExecuteAsync();
        }

        public void Delete(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);
            db.KeyDelete(key);
            RemoveIndex(entity);
        }

        public async Task DeleteAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);
            await db.KeyDeleteAsync(key);
            await RemoveIndexAsync(entity);
        }

        private void UpdateIndex(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetIndexKey();
            db.SortedSetAdd(key, entity.Id, 1);
        }

        private Task UpdateIndexAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetIndexKey();
            return db.SortedSetAddAsync(key, entity.Id, 1);
        }

        private void RemoveIndex(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetIndexKey();
            db.SortedSetRemove(key, entity.Id);
        }

        private Task RemoveIndexAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetIndexKey();
            return db.SortedSetRemoveAsync(key, entity.Id);
        }

        private RedisKey GetKey(string id)
        {
            return Name + ":" + id;
        }

        private RedisKey GetIndexKey()
        {
            return Name + ":index";
        }

        private void Migrate(uint from, uint to, T entity, Dictionary<string, object> extra)
        {
            throw new NotImplementedException();
        }

        private T Convert(RedisValue serialized)
        {
            throw new NotImplementedException();
        }

        private string Convert(T entity)
        {
            throw new NotImplementedException();
        }
    }
}