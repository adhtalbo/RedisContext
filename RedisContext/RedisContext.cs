namespace RedisContext
{
    using System.Configuration;
    using StackExchange.Redis;

    public abstract class RedisContext
    {
        public ConnectionMultiplexer ConnectionMultiplexer { get; private set; }

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

        private void Initialize()
        {
            var type = GetType();
            var properties = type.GetProperties();
            var redisSetType = typeof (RedisSet<>);

            foreach (var property in properties)
            {
                if (property.PropertyType.IsGenericType &&
                    property.PropertyType.GetGenericTypeDefinition() == redisSetType)
                {            
                    var entityType = property.PropertyType.GenericTypeArguments[0];

                    var setType = typeof(RedisSet<>).MakeGenericType(entityType);
                    var constructor = setType.GetConstructors()[0];

                    var value = constructor.Invoke(new object[] {this, property.Name});

                    property.SetValue(this, value);
                }
            }
        }
    }
}