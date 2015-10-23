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

    public sealed class RedisSet<T> where T : RedisEntity
    {
        public string Name { get; private set; }

        public RedisContext Context { get; private set; }

        private readonly MessagePackSerializer<T> _serializer;

        private readonly uint _version;

        private IDictionary<uint, IEnumerable<MethodInfo>> _migrations;

        public RedisSet(RedisContext ctx, string name)
        {
            Context = ctx;
            Name = name;

            var type = typeof(T);
            var versionAttr = type.GetCustomAttributes(typeof(VersionAttribute), true).FirstOrDefault() as VersionAttribute;
            _version = versionAttr == null ? 0 : versionAttr.Version;

            _serializer = Serializer.Create<T>();
            _migrations = new Dictionary<uint, IEnumerable<MethodInfo>>();

            for (uint i = 0; i < _version; i++)
            {
                _migrations.Add(i, GetMigrationPaths(i));
            }
        }

        private IEnumerable<MethodInfo> GetMigrationPaths(uint from = 0)
        {
            var type = typeof (T);
            var methods = type.GetMethods().Where(method => method.GetCustomAttributes(typeof(MigrateAttribute), true).Any());

            var startpoints = methods.Where(method => method.GetCustomAttributes(typeof(MigrateAttribute), true).Any(attr => ((MigrateAttribute) attr).From == from));

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

        public T Fetch(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(id);
            var serialized = db.HashGetAll(key);
            return ConvertEntity(serialized);
        }

        public async Task<T> FetchAsync(string id)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(id);
            var serialized = await db.HashGetAllAsync(key);
            return ConvertEntity(serialized);
        }

        public IEnumerable<T> Fetch(IEnumerable<string> ids)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var keys = ids.Select(GetKey);

            var tasks = keys.Select(key => db.HashGetAllAsync(key)).ToArray();
            Task.WaitAll(tasks);

            return tasks.Select(x => x.Result).Select(ConvertEntity);
        }

        public async Task<IEnumerable<T>> FetchAsync(IEnumerable<string> ids)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var keys = ids.Select(GetKey);
            var tasks = keys.Select(key => db.HashGetAllAsync(key)).ToArray();
            
            var results = await Task.WhenAll(tasks);
            return results.Select(ConvertEntity);
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

        public async Task<bool> InsertAsync(T entity)
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

        public void InsertOrReplace(T entity)
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

        public async Task InsertOrReplaceAsync(T entity)
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

        public bool Update(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            var etag = entity.Etag;
            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = ConvertEntity(entity);

            var tran = db.CreateTransaction();
            tran.AddCondition(Condition.HashEqual(key, "etag", etag));

            tran.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", entity.Version)
            });

            return tran.Execute();
        }

        public Task<bool> UpdateAsync(T entity)
        {
            var db = Context.ConnectionMultiplexer.GetDatabase();
            var key = GetKey(entity.Id);

            var etag = entity.Etag;
            entity.Etag = Guid.NewGuid().ToString("N");

            var serialized = ConvertEntity(entity);

            var tran = db.CreateTransaction();
            tran.AddCondition(Condition.HashEqual(key, "etag", etag));

            tran.HashSetAsync(key, new HashEntry[]
            {
                new HashEntry("etag", entity.Etag),
                new HashEntry("data", serialized),
                new HashEntry("version", entity.Version)
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

        private void Migrate(uint from, T entity, byte[] data)
        {
            var redisProperties = new EntityProperties(data);
            var methods = GetMigrationMethods(from);
            methods.ToList().ForEach(x => x.Invoke(entity, new object[] {redisProperties}));
        }

        private IEnumerable<MethodInfo> GetMigrationMethods(uint from)
        {
            return _migrations.ContainsKey(from) ? _migrations[from] : (IEnumerable<MethodInfo>) Enumerable.Empty<MemberInfo>();
        }

        private T ConvertEntity(HashEntry[] serialized)
        {
            uint version = 0;
            byte[] data = new byte[0];

            foreach (var item in serialized)
            {
                switch (item.Name)
                {
                    case "version":
                        version = (uint) Convert.ChangeType(item.Value, typeof(uint));
                        break;
                    case "date":
                        data = item.Value;
                        break;
                }
            }

            var unpacked = _serializer.UnpackSingleObject(data);

            if (version < _version)
            {
                Migrate(_version, unpacked, data);
            }

            return unpacked;
        }

        private RedisValue ConvertEntity(T entity)
        {
            return _serializer.PackSingleObject(entity);
        }
    }
}