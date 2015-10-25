namespace RedisContext.Entities
{
    public class IndexEntity : RedisEntity
    {
        public IndexEntity()
        {
        }

        public IndexEntity(RedisEntity entity, string indexValue)
        {
            Id = indexValue;
            EntityId = entity.Id;
        }

        public string EntityId { get; set; }
    }
}