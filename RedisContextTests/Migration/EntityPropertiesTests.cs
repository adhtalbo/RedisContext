namespace RedisContextTests.Migration
{
    using MsgPack.Serialization;
    using RedisContext.Migration;
    using TestClasses.Entities;
    using Xunit;

    public class EntityPropertiesTests
    {
        private MessagePackSerializer<BasicEntity> _serializer;

        public EntityPropertiesTests()
        {
            var context = new SerializationContext();
            context.SerializationMethod = SerializationMethod.Map;

            _serializer = context.GetSerializer<BasicEntity>();
        }

        [Fact]
        public void EntityProperties_SerializedClass_CanConvertToEntityProperties()
        {
            // Arrange
            var expectedId = "SomeId";
            var ob = new BasicEntity()
            {
                Id = expectedId
            };
            var data = _serializer.PackSingleObject(ob);

            // Act
            var entityProperties = new EntityProperties(data);
            var containsId = entityProperties.ContainsKey("Id");
            string returnedId;
            var isString = entityProperties["Id"].TryGet(out returnedId);
            

            // Assert
            Assert.True(containsId);
            Assert.True(isString);
            Assert.Equal(expectedId, returnedId);
        }
    }
}