namespace RedisContextTests.TestClasses
{
    using Entities;
    using RedisContext;

    public class BasicContextVersioned : RedisContext
    {
        public BasicContextVersioned()
            : base("localhost")
        {
        }

        public RedisSet<BasicEntityVersioned> Entity { get; set; }
    }
}