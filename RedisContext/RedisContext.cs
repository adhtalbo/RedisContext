namespace RedisContext
{
    using System.Configuration;
    using System.Reflection;
    using Mock;
    using StackExchange.Redis;

    public abstract class RedisContext
    {
        public RedisContext(ConnectionMultiplexer connectionMultiplexer)
        {
            ConnectionMultiplexer = connectionMultiplexer;
            Initialize();
        }

        public RedisContext(string connectionString)
        {
            if (HasConnectionString(connectionString))
            {
                connectionString = ConfigurationManager.ConnectionStrings[connectionString].ConnectionString;
            }
            ConnectionMultiplexer = ConnectionMultiplexer.Connect(connectionString);
            Initialize();
        }

        public ConnectionMultiplexer ConnectionMultiplexer { get; private set; }

        private static bool HasConnectionString(string key)
        {
            try
            {
                return ConfigurationManager.ConnectionStrings[key].ConnectionString.Length > 0;
            }
            catch
            {
                return false;
            }
        }

        internal void Initialize(bool mock = false)
        {
            var type = GetType();
            var methodName = "CreateRedisSet";
            if (mock)
            {
                methodName = "CreateMockRedisSet";
            }
            var createRedisSetMethodInfo = typeof (RedisContext).GetMethod(methodName,
                BindingFlags.Instance | BindingFlags.NonPublic);

            var properties = type.GetProperties();
            var redisSetType = typeof (RedisSet<>);

            foreach (var property in properties)
            {
                if (property.PropertyType.IsGenericType &&
                    property.PropertyType.GetGenericTypeDefinition() == redisSetType)
                {
                    var entityType = property.PropertyType.GetGenericArguments()[0];

                    var value = createRedisSetMethodInfo.MakeGenericMethod(entityType)
                        .Invoke(this, new object[] {this, property.Name});

                    property.SetValue(this, value);
                }
            }
        }

        private RedisSet<T> CreateRedisSet<T>(RedisContext ctx, string name) where T : RedisEntity
        {
            return new RedisSet<T>(ctx, name);
        }

        private RedisSet<T> CreateMockRedisSet<T>(RedisContext ctx, string name) where T : RedisEntity
        {
            return new RedisSetMock<T>(ctx, name);
        }
    }
}