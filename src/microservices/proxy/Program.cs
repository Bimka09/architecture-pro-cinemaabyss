using Microsoft.AspNetCore.Mvc;
using proxy_service;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddHttpClient();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.UseHttpsRedirection();

app.UseMiddleware<ErrorLoggingMiddleware>();
// ----------------- Health endpoint -----------------
app.MapGet("/health", () => Results.Ok("Healthy"))
   .WithName("Health")
   .WithOpenApi();

// ----------------- Movies proxy -----------------
app.Map("/api/movies/{*subPath}", async (HttpContext context,
                                        ILogger<Program> logger,
                                        IConfiguration configuration,
                                        IHttpClientFactory httpClientFactory) =>
{
    var pathAndQuery = $"{context.Request.Path}{context.Request.QueryString}";
    var moviesServiceUrl = $"{configuration["MOVIES_SERVICE_URL"]}{pathAndQuery}";
    var monolithUrl = $"{configuration["MONOLITH_URL"]}{pathAndQuery}";

    var gradualMigrationRaw = configuration["GRADUAL_MIGRATION"];
    var migrationPercentRaw = configuration["MOVIES_MIGRATION_PERCENT"];
    string targetUrl = monolithUrl;

    if (bool.TryParse(gradualMigrationRaw, out var gradualMigration) && gradualMigration)
    {
        if (int.TryParse(migrationPercentRaw, out var migrationPercent) &&
            migrationPercent >= 0 && migrationPercent <= 100)
        {
            var random = Random.Shared.Next(0, 100);
            targetUrl = random < migrationPercent ? monolithUrl : moviesServiceUrl;
        }
    }

    await ProxyRequest(httpClientFactory, context, targetUrl);
})
.WithName("movies")
.WithOpenApi();

// ----------------- Users proxy -----------------
app.Map("/api/users/{*subPath}", async (HttpContext context,
                                       IConfiguration configuration,
                                       IHttpClientFactory httpClientFactory) =>
{
    var pathAndQuery = $"{context.Request.Path}{context.Request.QueryString}";
    var targetUrl = $"{configuration["MONOLITH_URL"]}{pathAndQuery}";
    await ProxyRequest(httpClientFactory, context, targetUrl);
})
.WithName("users")
.WithOpenApi();

// ----------------- Events proxy -----------------
app.Map("/api/events/{*subPath}", async (HttpContext context,
                                       IConfiguration configuration,
                                       IHttpClientFactory httpClientFactory) =>
{
    var pathAndQuery = $"{context.Request.Path}{context.Request.QueryString}";
    var targetUrl = $"{configuration["EVENTS_SERVICE_URL"]}{pathAndQuery}";
    await ProxyRequest(httpClientFactory, context, targetUrl);
})
.WithName("events")
.WithOpenApi();



app.Run();

// ----------------- Proxy helper -----------------
static async Task ProxyRequest(IHttpClientFactory factory, HttpContext context, string targetUrl)
{
    var client = factory.CreateClient();
    var request = new HttpRequestMessage(new HttpMethod(context.Request.Method), targetUrl);

    // Копируем тело запроса
    if (context.Request.ContentLength > 0)
        request.Content = new StreamContent(context.Request.Body);

    // Копируем заголовки
    foreach (var header in context.Request.Headers)
    {
        if (!request.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray()))
            request.Content?.Headers.TryAddWithoutValidation(header.Key, header.Value.ToArray());
    }

    // Отправка запроса к целевому сервису
    using var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, context.RequestAborted);

    // Копируем статус код и заголовки сразу в HttpContext.Response
    context.Response.StatusCode = (int)response.StatusCode;
    foreach (var header in response.Headers)
        context.Response.Headers[header.Key] = header.Value.ToArray();
    foreach (var header in response.Content.Headers)
        context.Response.Headers[header.Key] = header.Value.ToArray();

    // Убираем transfer-encoding, чтобы избежать проблем
    context.Response.Headers.Remove("transfer-encoding");

    // Копируем тело ответа напрямую в response stream
    await response.Content.CopyToAsync(context.Response.Body);
}
