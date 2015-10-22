namespace RedisContext
{
    using System;

    public class VersionAttribute : Attribute
    {
        public uint Version { get; private set; }

        public VersionAttribute(uint version)
        {
            Version = version;
        }
    }
}