namespace RedisContextTests.TestClasses
{
    using RedisContext;

    public class ContextUsingConnectionString : RedisContext
    {
        public ContextUsingConnectionString() : base("RedisServer")
        {
        }
    }
}