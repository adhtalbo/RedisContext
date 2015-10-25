namespace RedisContextTests.TestClasses
{
    using Entities;
    using RedisContext;

    public class BasicContextVersioned : RedisContext
    {
        public BasicContextVersioned()
            : base("127.0.0.1")
        {
        }

        public RedisSet<BasicEntityVersioned> Entity { get; set; }
    }
}