namespace RedisContext
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;
    using Migration;
    using MsgPack.Serialization;
    using StackExchange.Redis;

    public class RedisSet<T> where T : RedisEntity
    {
        public string Name { get; private set; }

        public RedisContext Context { get; private set; }

        private readonly MessagePackSerializer<T> _serializer;

        private readonly uint _version;

        private IEnumerable<MethodInfo> _migrationMethods;

        private IDictionary<uint, IEnumerable<MethodInfo>> _migrations;

        internal RedisSet(RedisContext ctx, string name)
        {
            Context = ctx;
            Name = name;

            var type = typeof(T);
            var versionAttr = type.GetCustomAttributes(typeof(VersionAttribute), true).FirstOrDefault() as VersionAttribute;
            _version = versionAttr == null ? 0 : versionAttr.Version;

            _serializer = Serializer.Create<T>();

            var allMethods = type.GetMethods(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
            _migrationMethods = allMethods.Where(method => method.GetCustomAttributes(typeof(MigrateAttribute), true).Any());
            
            _migrations = new Dictionary<uint, IEnumerable<MethodInfo>>();

            for (uint i = 0; i < _version; i++)
            {
                _migrations.Add(i, GetMigrationPaths(i));
            }
        }

        private IEnumerable<MethodInfo> GetMigrationPaths(uint from = 0)
        {

            var startpoints = _migrationMethods.Where(method => method.GetCustomAttributes(typeof(MigrateAttribute), true).Any(attr => ((MigrateAttribute)attr).From == from));

            var paths = new List<List<MethodInfo>>();

            foreach (var startpoint in startpoints)
            {
                var end = ((MigrateAttribute) startpoint.GetCustomAttribute(typeof (MigrateAttribute), true)).To;

                if (end == _version)
                {
                    return new List<MethodInfo>() {startpoint};
                }

                var path = new List<MethodInfo>();
                path.Add(startpoint);
                path.AddRange(GetMigrationPaths(end));
                paths.Add(path);
            }

            return paths.DefaultIfEmpty(Enumerable.Empty<MethodInfo>()).OrderBy(x => x.Count()).FirstOrDefault();
        }

        public virtual T Fetch(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(id);
            var serialized = db.HashGetAll(key);
            return ConvertEntity(serialized);            
        }

        public virtual async Task<T> FetchAsync(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(id);
            var serialized = await db.HashGetAllAsync(key);
            return ConvertEntity(serialized);
        }

        public virtual IEnumerable<T> Fetch(IEnumerable<string> ids)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var keys = ids.Select(GetKey);

            var tasks = keys.Select(key => db.HashGetAllAsync(key)).ToArray();
            Task.WaitAll(tasks);

            return tasks.Select(x => x.Result).Select(ConvertEntity);
        }

        public virtual async Task<IEnumerable<T>> FetchAsync(IEnumerable<string> ids)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var keys = ids.Select(GetKey);
            var tasks = keys.Select(key => db.HashGetAllAsync(key)).ToArray();
            
            var results = await Task.WhenAll(tasks);
            return results.Select(ConvertEntity);
        }

        public virtual IEnumerable<T> Fetch(string id, int limit, int offset = 0)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var index = GetIndexKey();

            var keys = db.SortedSetRangeByValue(index, id, default(RedisValue), Exclude.None, offset, limit).Select(x => x.ToString());
            return Fetch(keys);
        }

        public virtual Task<IEnumerable<T>> FetchAsync(string id, int limit, int offset = 0)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var index = GetIndexKey();

            var keys = db.SortedSetRangeByValue(index, id, default(RedisValue), Exclude.None, offset, limit).Select(x => x.ToString());
            return FetchAsync(keys);
        }

        public virtual IEnumerable<T> Fetch(string min, string max)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var index = GetIndexKey();

            var keys = db.SortedSetRangeByValue(index, min, max).Select(x => x.ToString());
            return Fetch(keys);
        }

        public virtual Task<IEnumerable<T>> FetchAsync(string min, string max)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var index = GetIndexKey();

            var keys = db.SortedSetRangeByValue(index, min, max).Select(x => x.ToString());
            return FetchAsync(keys);
        }

        public virtual bool Insert(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = ConvertEntity(entity);

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

        public virtual async Task<bool> InsertAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = ConvertEntity(entity);

            var tran = db.CreateTransaction();
            tran.AddCondition(Condition.KeyNotExists(key));

            tran.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", entity.Version)
            });

            var success = await tran.ExecuteAsync();

            if (success)
            {
                await UpdateIndexAsync(entity);
            }

            return success;
        }

        public virtual void InsertOrReplace(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = ConvertEntity(entity);

            db.HashSet(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", entity.Version)
            });

            UpdateIndex(entity);
        }

        public virtual async Task InsertOrReplaceAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = ConvertEntity(entity);

            await db.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", entity.Version)
            });

            await UpdateIndexAsync(entity);
        }

        public virtual bool Update(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            var etag = entity.Etag;
            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = ConvertEntity(entity);

            var tran = db.CreateTransaction();
            tran.AddCondition(Condition.KeyExists(key));
            tran.AddCondition(Condition.HashEqual(key, "etag", etag));

            tran.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", entity.Version)
            });

            if (!tran.Execute())
            {
                entity.Etag = etag;
                return false;
            }
            return true;
        }

        public virtual async Task<bool> UpdateAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            var etag = entity.Etag;
            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = ConvertEntity(entity);

            var tran = db.CreateTransaction();
            tran.AddCondition(Condition.KeyExists(key));
            tran.AddCondition(Condition.HashEqual(key, "etag", etag));

            tran.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", entity.Version)
            });

            if (! await tran.ExecuteAsync())
            {
                entity.Etag = etag;
                return false;
            }
            return true;
        }

        public virtual void Delete(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);
            db.KeyDelete(key);
            RemoveIndex(entity);
        }

        public virtual async Task DeleteAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);
            await db.KeyDeleteAsync(key);
            await RemoveIndexAsync(entity);
        }

        public virtual void Delete(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(id);
            db.KeyDelete(key);
            RemoveIndex(id);
        }

        public virtual async Task DeleteAsync(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(id);
            await db.KeyDeleteAsync(key);
            await RemoveIndexAsync(id);
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

        private void RemoveIndex(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetIndexKey();
            db.SortedSetRemove(key, id);
        }

        private Task RemoveIndexAsync(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetIndexKey();
            return db.SortedSetRemoveAsync(key, id);
        }

        private RedisKey GetKey(string id)
        {
            return Name + ":" + id;
        }

        private RedisKey GetIndexKey()
        {
            return Name + ":index";
        }

        private void Migrate(uint from, T entity, byte[] data)
        {
            var redisProperties = new EntityProperties(data);
            var methods = GetMigrationMethods(from);
            methods.ToList().ForEach(x => x.Invoke(entity, new object[] {redisProperties}));
        }

        private IEnumerable<MethodInfo> GetMigrationMethods(uint from)
        {
            return _migrations.ContainsKey(from) ? _migrations[from] : Enumerable.Empty<MethodInfo>();
        }

        internal T ConvertEntity(HashEntry[] serialized)
        {
            if (!serialized.Any())
            {
                return null;
            }

            uint version = 0;
            byte[] data = new byte[0];

            foreach (var item in serialized)
            {
                switch (item.Name)
                {
                    case "version":
                        version = (uint) Convert.ChangeType(item.Value, typeof(uint));
                        break;
                    case "data":
                        data = item.Value;
                        break;
                }
            }

            var unpacked = _serializer.UnpackSingleObject(data);

            if (version < _version)
            {
                Migrate(version, unpacked, data);
            }

            return unpacked;
        }

        internal RedisValue ConvertEntity(T entity)
        {
            return _serializer.PackSingleObject(entity);
        }
    }
}