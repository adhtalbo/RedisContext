namespace RedisContext.Mock
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using StackExchange.Redis;

    public class RedisSetMock<T> : RedisSet<T> where T : RedisEntity
    {
        private readonly SortedDictionary<string, Hash> _entities;

        public RedisSetMock(RedisContext ctx, string name) : base(ctx, name)
        {
            _entities = new SortedDictionary<string, Hash>();
        }

        public override T Fetch(string id)
        {
            if (_entities.ContainsKey(id))
            {
                return ConvertEntity(_entities[id]);
            }
            return null;
        }

        public override Task<T> FetchAsync(string id)
        {
            return Task.FromResult(Fetch(id));
        }

        public override IEnumerable<T> Fetch(IEnumerable<string> ids)
        {
            return ids.Select(Fetch);
        }

        public override Task<IEnumerable<T>> FetchAsync(IEnumerable<string> ids)
        {
            return Task.FromResult(Fetch(ids));
        }

        public override IEnumerable<T> Fetch(string id, int limit, int offset = 0)
        {
            return
                _entities.Where(x => (string.Compare(x.Key, id, StringComparison.Ordinal) >= 0))
                    .Skip(offset)
                    .Take(limit)
                    .Select(x => Fetch(x.Key));
        }

        public override Task<IEnumerable<T>> FetchAsync(string id, int limit, int offset = 0)
        {
            return Task.FromResult(Fetch(id, limit, offset));
        }

        public override IEnumerable<T> Fetch(string min, string max)
        {
            return _entities
                .Where(x => (string.Compare(x.Key, min, StringComparison.Ordinal) >= 0))
                .Where(x => (string.Compare(x.Key, max, StringComparison.Ordinal) <= 0))
                .Select(x => Fetch(x.Key));
        }

        public override Task<IEnumerable<T>> FetchAsync(string min, string max)
        {
            return Task.FromResult(Fetch(min, max));
        }

        public override bool Insert(T entity)
        {
            if (_entities.ContainsKey(entity.Id))
            {
                return false;
            }

            entity.Etag = Guid.NewGuid().ToString("N");

            _entities[entity.Id] = new Hash
            {
                Data = ConvertEntity(entity),
                Etag = entity.Etag,
                Id = entity.Id
            };

            return true;
        }

        public override Task<bool> InsertAsync(T entity)
        {
            return Task.FromResult(Insert(entity));
        }

        public override void InsertOrReplace(T entity)
        {
            entity.Etag = Guid.NewGuid().ToString("N");

            _entities[entity.Id] = new Hash
            {
                Data = ConvertEntity(entity),
                Etag = entity.Etag,
                Id = entity.Id
            };
        }

        public override Task InsertOrReplaceAsync(T entity)
        {
            InsertOrReplace(entity);
            return Task.FromResult(false);
        }

        public override bool Update(T entity)
        {
            if (!_entities.ContainsKey(entity.Id) || _entities[entity.Id].Etag != entity.Etag)
            {
                return false;
            }

            entity.Etag = Guid.NewGuid().ToString("N");

            _entities[entity.Id] = new Hash
            {
                Data = ConvertEntity(entity),
                Etag = entity.Etag,
                Id = entity.Id
            };

            return true;
        }

        public override Task<bool> UpdateAsync(T entity)
        {
            return Task.FromResult(Update(entity));
        }

        public override bool Replace(T entity)
        {
            if (!_entities.ContainsKey(entity.Id))
            {
                return false;
            }

            entity.Etag = Guid.NewGuid().ToString("N");

            _entities[entity.Id] = new Hash
            {
                Data = ConvertEntity(entity),
                Etag = entity.Etag,
                Id = entity.Id
            };

            return true;
        }

        public override Task<bool> ReplaceAsync(T entity)
        {
            return Task.FromResult(Replace(entity));
        }

        public override void Delete(T entity)
        {
            _entities.Remove(entity.Id);
        }

        public override Task DeleteAsync(T entity)
        {
            Delete(entity);
            return Task.FromResult(false);
        }

        public override void Delete(string id)
        {
            _entities.Remove(id);
        }

        public override Task DeleteAsync(string id)
        {
            Delete(id);
            return Task.FromResult(false);
        }

        private T ConvertEntity(Hash hash)
        {
            if (hash == null)
            {
                return null;
            }
            return ConvertEntity(hash.GetHashEntry());
        }

        public static T MigrateFrom<TF>(TF from) where TF : RedisEntity
        {
            var oldSet = new RedisSetMock<TF>(null, "");
            var newSet = new RedisSetMock<T>(null, "");

            var data = oldSet.ConvertEntity(from);
            return newSet.ConvertEntity(new Hash
            {
                Data = data,
                Id = from.Id,
                Etag = ""
            });
        }

        private class Hash
        {
            public string Etag { get; set; }
            public string Id { get; set; }
            public byte[] Data { get; set; }

            public HashEntry[] GetHashEntry()
            {
                return new[]
                {
                    new HashEntry("etag", Etag),
                    new HashEntry("id", Id),
                    new HashEntry("data", Data)
                };
            }
        }
    }
}