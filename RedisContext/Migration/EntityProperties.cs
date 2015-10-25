namespace RedisContext.Migration
{
    using System.Collections.Generic;
    using MsgPack;
    using MsgPack.Serialization;

    public class EntityProperties : Dictionary<string, EntityProperty>
    {
        private static readonly MessagePackSerializer<Dictionary<MessagePackObject, MessagePackObject>> _serializer;

        static EntityProperties()
        {
            _serializer = Serializer.Create<Dictionary<MessagePackObject, MessagePackObject>>();
        }

        public EntityProperties(byte[] bytes)
        {
            var properties = _serializer.UnpackSingleObject(bytes);
            foreach (var property in properties)
            {
                Add((string) property.Key, new EntityProperty(property.Value));
            }
        }
    }
}