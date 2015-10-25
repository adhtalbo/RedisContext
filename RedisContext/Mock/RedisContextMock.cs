namespace RedisContext.Mock
{
    using System.Runtime.Serialization;

    public static class RedisContextMock<T> where T : RedisContext
    {
        public static T Create()
        {
            var instance = (T) FormatterServices.GetUninitializedObject(typeof (T));
            instance.Initialize(true);
            return instance;
        }
    }
}