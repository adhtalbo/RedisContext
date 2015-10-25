namespace RedisContext.Migration
{
    using System;

    public class MigrateAttribute : Attribute
    {
        public MigrateAttribute(uint from, uint to)
        {
            From = from;
            To = to;
        }

        public uint From { get; private set; }

        public uint To { get; private set; }
    }
}