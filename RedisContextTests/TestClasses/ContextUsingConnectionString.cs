namespace RedisContextTests.TestClasses
{
    using RedisContext;
    using StackExchange.Redis;

    public class ContextUsingConnectionString : RedisContext
    {
        public ContextUsingConnectionString() : base("RedisServer")
        {
        }
    }
}