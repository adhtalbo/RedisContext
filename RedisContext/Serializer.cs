namespace RedisContext
{
    using MsgPack.Serialization;

    internal static class Serializer
    {
        public static MessagePackSerializer<T> Create<T>()
        {
            var context = new SerializationContext();
            context.SerializationMethod = SerializationMethod.Map;

            return context.GetSerializer<T>();
        }
    }
}