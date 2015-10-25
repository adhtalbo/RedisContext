namespace RedisContextTests.TestClasses.Entities
{
    using RedisContext;
    using RedisContext.Migration;

    [Version(5)]
    public class BasicEntityVersioned : RedisEntity
    {
        public string StringValueRename { get; set; }

        public string StringValuePrefixed { get; set; }

        public string StringValueSuffixed { get; set; }

        [Migrate(0, 1)]
        private void MigrateToV1(EntityProperties properties)
        {
            string stringValueClone;
            properties["StringValue"].TryGet(out stringValueClone);
            StringValueRename = stringValueClone;
        }

        [Migrate(1, 2)]
        private void MigrateToV2(EntityProperties properties)
        {
            string stringValuePrefix;
            properties["StringValue"].TryGet(out stringValuePrefix);
            StringValuePrefixed = "Prefix_" + stringValuePrefix;
        }

        [Migrate(2, 5)]
        private void MigrateToV5(EntityProperties properties)
        {
            string stringValueSuffixed;
            properties["StringValue"].TryGet(out stringValueSuffixed);
            StringValueSuffixed = stringValueSuffixed + "_Suffix";
        }
    }
}