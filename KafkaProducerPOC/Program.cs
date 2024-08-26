using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
var kafkaUrl = builder.Configuration["Kafka:BootstrapServers"];
var producerConfig = new ProducerConfig { BootstrapServers = kafkaUrl };
using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", async (string topic) =>
{
    var forecast = Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    var msg = new Message<Null, string> { Value = JsonSerializer.Serialize(forecast) };
    await producer.ProduceAsync(topic, msg);
    return forecast;
})
.WithName("GetWeatherForecast")
.WithOpenApi();
app.MapPost("kafkaProducer", async (string topic, object message) =>
{
    try
    {
        var adminClientConfig = new AdminClientConfig
        {
            BootstrapServers = kafkaUrl
        };
        using var adminClient = new AdminClientBuilder(adminClientConfig).Build();
        var metadata = adminClient.GetMetadata(topic, TimeSpan.FromSeconds(10));
        //Create Topic if not exists        
        if (metadata.Topics.Count == 0)
            adminClient.CreateTopicsAsync(new TopicSpecification[] { new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 } }).Wait();
        var msg = new Message<Null, string> { Value = JsonSerializer.Serialize(message) };
        await producer.ProduceAsync(topic, msg);
        producer.Flush(TimeSpan.FromMinutes(1));
        producerConfig.RetryBackoffMs = 1000;
        producerConfig.MessageTimeoutMs = 5000;
        producerConfig.EnableIdempotence = true;
        return Results.Ok(new
        {
            obj = message,
            message = $"Message sent to kafka topic {topic} successfully",
        });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(ex.Message);
    }
});
app.Run();

internal record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
