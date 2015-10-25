namespace RedisContext.Extensions
{
    using System.Collections.Generic;
    using System.Linq;
    using Entities;

    public static class IndexEntityExtensions
    {
        public static IEnumerable<string> GetIds(this IEnumerable<IndexEntity> index)
        {
            return index.Select(x => x.EntityId);
        } 
    }
}