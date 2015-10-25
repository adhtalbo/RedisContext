namespace RedisContext.Migration
{
    using System;
    using System.Globalization;
    using MsgPack;

    public class EntityProperty : IConvertible
    {
        private MessagePackObject _messagePackObject;

        public EntityProperty(MessagePackObject obj)
        {
            _messagePackObject = obj;
        }

        TypeCode IConvertible.GetTypeCode()
        {
            return Type.GetTypeCode(_messagePackObject.UnderlyingType);
        }

        bool IConvertible.ToBoolean(IFormatProvider provider)
        {
            return _messagePackObject.AsBoolean();
        }

        char IConvertible.ToChar(IFormatProvider provider)
        {
            return (char) _messagePackObject.AsByte();
        }

        sbyte IConvertible.ToSByte(IFormatProvider provider)
        {
            return _messagePackObject.AsSByte();
        }

        byte IConvertible.ToByte(IFormatProvider provider)
        {
            return _messagePackObject.AsByte();
        }

        short IConvertible.ToInt16(IFormatProvider provider)
        {
            return _messagePackObject.AsInt16();
        }

        ushort IConvertible.ToUInt16(IFormatProvider provider)
        {
            return _messagePackObject.AsUInt16();
        }

        int IConvertible.ToInt32(IFormatProvider provider)
        {
            return _messagePackObject.AsInt32();
        }

        uint IConvertible.ToUInt32(IFormatProvider provider)
        {
            return _messagePackObject.AsUInt32();
        }

        long IConvertible.ToInt64(IFormatProvider provider)
        {
            return _messagePackObject.AsInt64();
        }

        ulong IConvertible.ToUInt64(IFormatProvider provider)
        {
            return _messagePackObject.AsUInt64();
        }

        float IConvertible.ToSingle(IFormatProvider provider)
        {
            return _messagePackObject.AsSingle();
        }

        double IConvertible.ToDouble(IFormatProvider provider)
        {
            return _messagePackObject.AsDouble();
        }

        decimal IConvertible.ToDecimal(IFormatProvider provider)
        {
            return Convert.ToDecimal(_messagePackObject.AsString());
        }

        DateTime IConvertible.ToDateTime(IFormatProvider provider)
        {
            var data = _messagePackObject.AsInt64();
            return DateTime.FromBinary(data);
        }

        string IConvertible.ToString(IFormatProvider provider)
        {
            return _messagePackObject.AsString();
        }

        object IConvertible.ToType(Type conversionType, IFormatProvider provider)
        {
            return Convert.ChangeType(_messagePackObject.ToObject(), conversionType);
        }

        public bool? IsTypeOf<T>()
        {
            switch (Type.GetTypeCode(typeof (T)))
            {
                // Chars are bytes.
                case TypeCode.Char:
                    return _messagePackObject.IsTypeOf<byte>();

                // Decimals are strings.
                case TypeCode.Decimal:
                    double output;
                    return _messagePackObject.IsTypeOf<string>().HasValue
                           &&
                           _messagePackObject.IsTypeOf<string>().Value
                           &&
                           double.TryParse(_messagePackObject.AsString(), NumberStyles.Any,
                               NumberFormatInfo.InvariantInfo, out output);

                case TypeCode.DateTime:
                    return _messagePackObject.IsTypeOf<long>();
                default:
                    return _messagePackObject.IsTypeOf<T>();
            }
        }

        public bool TryGet<T>(out T value)
        {
            try
            {
                value = (T) Convert.ChangeType(this, typeof (T));
                return true;
            }
            catch (InvalidCastException)
            {
                value = default(T);
                return false;
            }
            catch (InvalidOperationException)
            {
                value = default(T);
                return false;
            }
        }
    }
}