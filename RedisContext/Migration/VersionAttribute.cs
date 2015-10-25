namespace RedisContext.Migration
{
    using System;

    public class VersionAttribute : Attribute
    {
        public VersionAttribute(uint version)
        {
            Version = version;
        }

        public uint Version { get; private set; }
    }
}