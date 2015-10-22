namespace RedisContext
{
    using System;

    public class MigrateAttribute : Attribute
    {
        public uint From { get; private set; }

        public uint To { get; private set; }

        public MigrateAttribute(uint from, uint to)
        {
            From = from;
            To = to;
        }
    }
}