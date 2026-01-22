using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHttpClient();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.UseHttpsRedirection();


app.MapGet("/api/movies", async (
    [FromServices] ILogger<Program> logger,
    [FromServices] IConfiguration configuration,
    [FromServices] IHttpClientFactory httpClientFactory) =>
{
    var gradualMigrationRaw = configuration["GRADUAL_MIGRATION"];
    var migrationPercentRaw = configuration["MOVIES_MIGRATION_PERCENT"];

    var MOVIES_SERVICE_URL = $"{configuration["MOVIES_SERVICE_URL"]}/api/movies";
    var MONOLITH_URL = $"{configuration["MONOLITH_URL"]}/api/movies";

    logger.LogInformation($"Gradual migration is {gradualMigrationRaw}");
    logger.LogInformation($"Migration percent is {migrationPercentRaw}");

    if (!bool.TryParse(gradualMigrationRaw, out var gradualMigration) || !gradualMigration)
    {
        return await ProxyGet(httpClientFactory, MONOLITH_URL);
    }

    if (!int.TryParse(migrationPercentRaw, out var migrationPercent) ||
        migrationPercent < 0 || migrationPercent > 100)
    {
        logger.LogError("Invalid MOVIES_MIGRATION_PERCENT: {Value}", migrationPercentRaw);
        return Results.Problem("Invalid migration percent configuration");
    }

    var random = Random.Shared.Next(0, 100);
    var targetUrl = random < migrationPercent
        ? MONOLITH_URL
        : MOVIES_SERVICE_URL;

    return await ProxyGet(httpClientFactory, targetUrl);
})
.WithName("Movies")
.WithOpenApi();

app.MapGet("/api/users", async (
    [FromServices] ILogger<Program> logger,
    [FromServices] IConfiguration configuration,
    [FromServices] IHttpClientFactory httpClientFactory) =>
{
    var MONOLITH_URL = $"{configuration["MONOLITH_URL"]}/api/users";
   
    return await ProxyGet(httpClientFactory, MONOLITH_URL);
})
.WithName("Users")
.WithOpenApi();

app.MapGet("/health", () =>
{
    return;
})
.WithName("Health")
.WithOpenApi();

static async Task<IResult> ProxyGet(IHttpClientFactory factory, string url)
{
    var client = factory.CreateClient();
    var response = await client.GetAsync(url);

    var content = await response.Content.ReadAsStringAsync();

    return Results.Content(
        content,
        response.Content.Headers.ContentType?.ToString(),
        statusCode: (int)response.StatusCode);
}

app.Run();
