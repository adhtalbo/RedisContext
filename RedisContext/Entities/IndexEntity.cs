﻿namespace RedisContext.Entities
{
    public class IndexEntity : RedisEntity
    {
        public IndexEntity() { }

        public IndexEntity(RedisEntity entity, string indexValue)
        {
            Id = indexValue;
            ElementId = entity.Id;
        }

        public string ElementId { get; set; }
    }
}