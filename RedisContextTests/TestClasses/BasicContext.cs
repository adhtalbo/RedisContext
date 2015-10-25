namespace RedisContextTests.TestClasses
{
    using Entities;
    using RedisContext;

    public class BasicContext : RedisContext
    {
        public BasicContext() : base("127.0.0.1")
        {
        }

        public RedisSet<BasicEntity> Entity { get; set; }
    }
}