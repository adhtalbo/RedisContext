namespace RedisContextTests
{
    using TestClasses;
    using Xunit;

    public class RedisContextTests
    {
        [Fact]
        public void RedisContext_CanConnectWithConnectionString()
        {
            // Arrange

            // Act
            var context = new BasicContext();

            // Assert
            Assert.True(context.ConnectionMultiplexer.IsConnected);
        }

        [Fact]
        public void RedisContext_CanConnectWithConfigValue()
        {
            // Arrange

            // Act
            var context = new ContextUsingConnectionString();

            // Assert
            Assert.True(context.ConnectionMultiplexer.IsConnected);
        }
    }
}