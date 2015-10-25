# Redis Context

This is a project that allows Redis to be used a Document Database. In usage it provides a single class from which all entities can be accessed.

## Example
First entities must be defined 
```c#
public class BasicEntity : RedisEntity
{
    public string SomeValue {get; set;}
    
    public int AnotherValue {get; set;}
}
```
Then a context must be defined
```c#
public class ExampleContext : RedisContext
{
    // Can be connection string OR the name of a connection string in the config
    public ExampleContext(string connectionString) : base(connectionString)
    {
    }
    
    public RedisSet<BasicEntity> BasicEntity {get; set;}
}
```
From here you can initialize and use the context
```c#
var context = new ExampleContext("127.0.0.1");

var entity = new BasicEntity(){
    // An Id MUST be provided for each entity
    Id = Guid.NewGuid().ToString("N"),
    SomeValue = "Hello World",
    AnotherValue = 42
};

context.Entity.Insert(entity);
```