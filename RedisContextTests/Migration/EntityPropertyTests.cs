namespace RedisContextTests.Migration
{
    using System;
    using MsgPack;
    using MsgPack.Serialization;
    using RedisContext.Migration;
    using Xunit;

    public class EntityPropertyTests
    {
        private readonly MessagePackSerializer<MessagePackObject> _unserializer;

        public EntityPropertyTests()
        {
            _unserializer = SerializationContext.Default.GetSerializer<MessagePackObject>();
        }

        private EntityProperty ToEntityProperty<T>(T input)
        {
            var serializer = SerializationContext.Default.GetSerializer<T>();
            var serialized = serializer.PackSingleObject(input);
            var unserialized = _unserializer.UnpackSingleObject(serialized);
            return new EntityProperty(unserialized);
        }

        [Fact]
        public void EntityProperty_CanConvertToBool()
        {
            // Arrange
            var expected = true;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isBool = entityProperty.IsTypeOf<bool>();
            var boolValue = Convert.ChangeType(entityProperty, TypeCode.Boolean);

            // Assert
            Assert.True(isBool);
            Assert.Equal(expected, boolValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToChar()
        {
            // Arrange
            var expected = 'f';
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isChar = entityProperty.IsTypeOf<char>();
            var charValue = Convert.ChangeType(entityProperty, TypeCode.Char);

            // Assert
            Assert.True(isChar);
            Assert.Equal(expected, charValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToSByte()
        {
            // Arrange
            var expected = (sbyte) 's';
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isSByte = entityProperty.IsTypeOf<sbyte>();
            var sbyteValue = Convert.ChangeType(entityProperty, TypeCode.SByte);

            // Assert
            Assert.True(isSByte);
            Assert.Equal(expected, sbyteValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToByte()
        {
            // Arrange
            var expected = (byte) 'b';
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isByte = entityProperty.IsTypeOf<byte>();
            var byteValue = Convert.ChangeType(entityProperty, TypeCode.Byte);

            // Assert
            Assert.True(isByte);
            Assert.Equal(expected, byteValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToInt16()
        {
            // Arrange
            var expected = (short) 5;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isShort = entityProperty.IsTypeOf<short>();
            var shortValue = Convert.ChangeType(entityProperty, TypeCode.Int16);

            // Assert
            Assert.True(isShort);
            Assert.Equal(expected, shortValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToUInt16()
        {
            // Arrange
            var expected = (ushort) 5;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isUShort = entityProperty.IsTypeOf<ushort>();
            var ushortValue = Convert.ChangeType(entityProperty, TypeCode.UInt16);

            // Assert
            Assert.True(isUShort);
            Assert.Equal(expected, ushortValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToInt32()
        {
            // Arrange
            var expected = 5;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isInt = entityProperty.IsTypeOf<int>();
            var intValue = Convert.ChangeType(entityProperty, TypeCode.Int32);

            // Assert
            Assert.True(isInt);
            Assert.Equal(expected, intValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToUInt32()
        {
            // Arrange
            var expected = (uint) 5;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isUInt = entityProperty.IsTypeOf<uint>();
            var uintValue = Convert.ChangeType(entityProperty, TypeCode.UInt32);

            // Assert
            Assert.True(isUInt);
            Assert.Equal(expected, uintValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToInt64()
        {
            // Arrange
            var expected = (long) 5;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isLong = entityProperty.IsTypeOf<long>();
            var longValue = Convert.ChangeType(entityProperty, TypeCode.Int64);

            // Assert
            Assert.True(isLong);
            Assert.Equal(expected, longValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToUInt64()
        {
            // Arrange
            var expected = (ulong) 5;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isULong = entityProperty.IsTypeOf<ulong>();
            var ulongValue = Convert.ChangeType(entityProperty, TypeCode.UInt64);

            // Assert
            Assert.True(isULong);
            Assert.Equal(expected, ulongValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToSingle()
        {
            // Arrange
            var expected = (float) 3.14;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isSingle = entityProperty.IsTypeOf<float>();
            var singleValue = Convert.ChangeType(entityProperty, TypeCode.Single);

            // Assert
            Assert.True(isSingle);
            Assert.Equal(expected, singleValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToDouble()
        {
            // Arrange
            var expected = 3.14;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isDouble = entityProperty.IsTypeOf<double>();
            var doubleValue = Convert.ChangeType(entityProperty, TypeCode.Double);

            // Assert
            Assert.True(isDouble);
            Assert.Equal(expected, doubleValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToDecimal()
        {
            // Arrange
            var expected = (decimal) 3.14;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isDecimal = entityProperty.IsTypeOf<decimal>();
            var decimalValue = Convert.ChangeType(entityProperty, TypeCode.Decimal);

            // Assert
            Assert.True(isDecimal);
            Assert.Equal(expected, decimalValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToToDateTime()
        {
            // Arrange
            var expected = DateTime.Now;
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isDateTime = entityProperty.IsTypeOf<DateTime>();
            var datetimeValue = Convert.ChangeType(entityProperty, TypeCode.DateTime);

            // Assert
            Assert.True(isDateTime);
            Assert.Equal(expected, datetimeValue);
        }

        [Fact]
        public void EntityProperty_CanConvertToString()
        {
            // Arrange
            var expected = "SomeTestString";
            var entityProperty = ToEntityProperty(expected);

            // Act
            var isString = entityProperty.IsTypeOf<string>();
            var stringValue = Convert.ChangeType(entityProperty, TypeCode.String);

            // Assert
            Assert.True(isString);
            Assert.Equal(expected, stringValue);
        }

        [Fact]
        public void EntityProperty_CanTryGet_WithValidType()
        {
            // Arrange
            var expected = "SomeTestString";
            var entityProperty = ToEntityProperty(expected);

            // Act
            string stringValue;
            var isString = entityProperty.TryGet(out stringValue);

            // Assert
            Assert.True(isString);
            Assert.Equal(expected, stringValue);
        }

        [Fact]
        public void EntityProperty_CanTryGet_WithInvalidType()
        {
            // Arrange
            var expected = 5;
            var entityProperty = ToEntityProperty(expected);

            // Act
            string stringValue;
            var isString = entityProperty.TryGet(out stringValue);

            // Assert
            Assert.False(isString);
            Assert.Null(stringValue);
        }
    }
}