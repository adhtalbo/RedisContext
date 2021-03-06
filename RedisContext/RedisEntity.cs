namespace RedisContext
{
    using System;
    using System.Linq;
    using Migration;

    [Serializable]
    public abstract class RedisEntity
    {
        public string Etag { get; internal set; }

        public string Id { get; set; }

        public uint Version
        {
            get
            {
                var type = GetType();
                var version =
                    type.GetCustomAttributes(typeof (VersionAttribute), true).FirstOrDefault() as VersionAttribute;
                return version == null ? 0 : version.Version;
            }
        }
    }
}