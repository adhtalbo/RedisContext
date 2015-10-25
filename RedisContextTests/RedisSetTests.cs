namespace RedisContextTests
{
    using System;
    using System.Linq;
    using System.Threading.Tasks;
    using TestClasses;
    using TestClasses.Entities;
    using Xunit;

    public class RedisSetTests
    {
        private readonly BasicContext _context;

        private readonly BasicContextVersioned _contextVersioned;

        public RedisSetTests()
        {
            _context = new BasicContext();
            _contextVersioned = new BasicContextVersioned();
        }

        [Fact]
        public async Task RedisSet_CanSetAndFetch()
        {
            // Arrange
            var expected = new BasicEntity
            {
                Id = "RedisSet_CanSetAndFetch"
            };

            // Act
            await _context.Entity.InsertAsync(expected);
            var returned = await _context.Entity.FetchAsync(expected.Id);

            // Assert
            Assert.Equal(expected.Id, returned.Id);

            // Cleanup
            await _context.Entity.DeleteAsync(expected);
        }

        [Fact]
        public async Task RedisSet_CanDelete()
        {
            // Arrange
            var expected = new BasicEntity
            {
                Id = "RedisSet_CanDelete"
            };

            // Act
            await _context.Entity.InsertAsync(expected);
            var returned = await _context.Entity.FetchAsync(expected.Id);
            await _context.Entity.DeleteAsync(expected);
            var returnedAfterDelete = await _context.Entity.FetchAsync(expected.Id);


            // Assert
            Assert.Equal(expected.Id, returned.Id);
            Assert.Null(returnedAfterDelete);
        }

        [Fact]
        public async Task RedisSet_CanFetchBatch_ByIds()
        {
            // Arrange
            Func<int, string> idGen = i => "A_RedisSet_CanFetchBatch_ByIds_" + i.ToString("D5");
            var entities = Enumerable.Range(0, 100).Select(i => new BasicEntity
            {
                Id = idGen(i)
            });
            await Task.WhenAll(entities.Select(e => _context.Entity.InsertAsync(e)));

            // Act
            var returned = await _context.Entity.FetchAsync(Enumerable.Range(0, 100).Select(i => idGen(i)));

            // Assert
            Assert.Equal(100, returned.Count());
            Enumerable.Range(0, 100).ToList().ForEach(i =>
                Assert.True(returned.Any(e => e.Id == idGen(i)))
                );

            // Cleanup
            await Task.WhenAll(entities.Select(e => _context.Entity.DeleteAsync(e)));
        }

        [Fact]
        public async Task RedisSet_CanFetchBatch_ByIdAndLimit()
        {
            // Arrange
            Func<int, string> idGen = i => "B_RedisSet_CanFetchBatch_ByIdAndLimit_" + i.ToString("D5");
            var entities = Enumerable.Range(0, 100).Select(i => new BasicEntity
            {
                Id = idGen(i)
            });
            await Task.WhenAll(entities.Select(e => _context.Entity.InsertAsync(e)));

            // Act
            var returned = await _context.Entity.FetchAsync(idGen(50), 50);

            // Assert
            Assert.Equal(50, returned.Count());
            Enumerable.Range(50, 50).ToList().ForEach(i =>
                Assert.True(returned.Any(e => e.Id == idGen(i)), string.Format("Id {0} not found in results", i))
                );

            // Cleanup
            await Task.WhenAll(entities.Select(e => _context.Entity.DeleteAsync(e)));
        }

        [Fact]
        public async Task RedisSet_CanFetchBatch_ByIdLimitAndOffset()
        {
            // Arrange
            Func<int, string> idGen = i => "C_RedisSet_CanFetchBatch_ByIdLimitAndOffset_" + i.ToString("D5");
            var entities = Enumerable.Range(0, 100).Select(i => new BasicEntity
            {
                Id = idGen(i)
            });
            await Task.WhenAll(entities.Select(e => _context.Entity.InsertAsync(e)));

            // Act
            var returned = await _context.Entity.FetchAsync(idGen(0), 50, 50);

            var ids = returned.Select(x => x.Id).ToArray();

            // Assert
            Assert.Equal(50, returned.Count());
            Enumerable.Range(50, 50).ToList().ForEach(i =>
                Assert.True(returned.Any(e => e.Id == idGen(i)), string.Format("Id {0} not found in results", i))
                );

            // Cleanup
            await Task.WhenAll(entities.Select(e => _context.Entity.DeleteAsync(e)));
        }

        [Fact]
        public async Task RedisSet_CanFetchBatch_ByMinAndMaxID()
        {
            // Arrange
            Func<int, string> idGen = i => "D_RedisSet_CanFetchBatch_ByMinAndMaxID_" + i.ToString("D5");
            var entities = Enumerable.Range(0, 100).Select(i => new BasicEntity
            {
                Id = idGen(i)
            });
            await Task.WhenAll(entities.Select(e => _context.Entity.InsertAsync(e)));

            // Act
            var returned = await _context.Entity.FetchAsync(idGen(10), idGen(59));

            // Assert
            Assert.Equal(50, returned.Count());
            Enumerable.Range(10, 50).ToList().ForEach(i =>
                Assert.True(returned.Any(e => e.Id == idGen(i)), string.Format("Id {0} not found in results", i))
                );

            // Cleanup
            await Task.WhenAll(entities.Select(e => _context.Entity.DeleteAsync(e)));
        }

        [Fact]
        public async Task RedisSet_Insert_CannotOverwrite()
        {
            // Arrange
            var entity = new BasicEntity
            {
                Id = "RedisSet_Insert_CannotOverwrite"
            };

            // Act
            var canInsert = await _context.Entity.InsertAsync(entity);
            var canOverwrite = await _context.Entity.InsertAsync(entity);

            // Assert
            Assert.True(canInsert, "Insert not successful");
            Assert.False(canOverwrite, "Can Overwrite");

            // Cleanup
            await _context.Entity.DeleteAsync(entity);
        }

        [Fact]
        public async Task RedisSet_InsertOrReplace_CanOverwrite()
        {
            // Arrange
            var id = "RedisSet_InsertOrReplace_CanOverwrite";
            var entityOld = new BasicEntity
            {
                Id = id,
                StringValue = "Original"
            };
            var entityNew = new BasicEntity
            {
                Id = id,
                StringValue = "Replacement"
            };

            // Act
            await _context.Entity.InsertOrReplaceAsync(entityOld);
            await _context.Entity.InsertOrReplaceAsync(entityNew);
            var returned = await _context.Entity.FetchAsync(id);

            // Assert
            Assert.Equal(entityNew.Id, returned.Id);
            Assert.Equal(entityNew.StringValue, returned.StringValue);

            // Cleanup
            await _context.Entity.DeleteAsync(entityNew);
        }

        [Fact]
        public async Task RedisSet_Update_CannotInsertMissingItem()
        {
            // Arrange
            var entity = new BasicEntity
            {
                Id = "RedisSet_Update_CannotInsertMissingItem"
            };

            // Act
            var success = await _context.Entity.UpdateAsync(entity);
            var returned = await _context.Entity.FetchAsync(entity.Id);

            // Assert
            Assert.False(success, "Update created missing item");
            Assert.Null(returned);

            // Cleanup
            await _context.Entity.DeleteAsync(entity);
        }

        [Fact]
        public async Task RedisSet_Update_CanUpdateExistingItem()
        {
            // Arrange
            var entity = new BasicEntity
            {
                Id = "RedisSet_Update_CanUpdateExistingItem",
                StringValue = "Original"
            };

            // Act
            await _context.Entity.InsertOrReplaceAsync(entity);
            entity.StringValue = "Repalcement";
            var success = await _context.Entity.UpdateAsync(entity);
            var returned = await _context.Entity.FetchAsync(entity.Id);

            // Assert
            Assert.True(success);
            Assert.Equal(entity.Id, returned.Id);
            Assert.Equal(entity.StringValue, returned.StringValue);

            // Cleanup
            await _context.Entity.DeleteAsync(entity);
        }

        [Fact]
        public async Task RedisSet_Update_CannotUpdateChangedItem()
        {
            // Arrange
            var entity = new BasicEntity
            {
                Id = "RedisSet_Update_CannotUpdateChangedItem",
                StringValue = "Original"
            };

            // Act
            await _context.Entity.InsertOrReplaceAsync(entity);

            var retrieved = await _context.Entity.FetchAsync(entity.Id);
            retrieved.StringValue = "Replacement";
            var canUpdate = await _context.Entity.UpdateAsync(retrieved);

            entity.StringValue = "New Replacement";
            var canUpdateChangedItem = await _context.Entity.UpdateAsync(entity);
            var returned = await _context.Entity.FetchAsync(entity.Id);

            // Assert
            Assert.True(canUpdate);
            Assert.False(canUpdateChangedItem);
            Assert.Equal(retrieved.Id, returned.Id);
            Assert.Equal(retrieved.StringValue, returned.StringValue);

            // Cleanup
            await _context.Entity.DeleteAsync(entity);
        }

        [Fact]
        public async Task RedisSet_Migrate_MigrationFunctionsGetsCalled()
        {
            // Arrange
            var entity = new BasicEntity
            {
                Id = "RedisSet_Migrate_MigrationFunctionsGetsCalled",
                StringValue = "SomeValue"
            };

            // Act
            await _context.Entity.InsertOrReplaceAsync(entity);
            var result = await _contextVersioned.Entity.FetchAsync(entity.Id);

            // Assert
            Assert.Equal(entity.Id, result.Id);
            Assert.Equal(entity.StringValue, result.StringValueRename);
            Assert.Equal("Prefix_" + entity.StringValue, result.StringValuePrefixed);
            Assert.Equal(entity.StringValue + "_Suffix", result.StringValueSuffixed);
        }
    }
}