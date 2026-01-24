using System.Text;

namespace proxy_service;

public class ErrorLoggingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ErrorLoggingMiddleware> _logger;

    public ErrorLoggingMiddleware(
        RequestDelegate next,
        ILogger<ErrorLoggingMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        // Разрешаем повторное чтение body
        context.Request.EnableBuffering();

        string requestBody = string.Empty;

        if (context.Request.ContentLength > 0)
        {
            context.Request.Body.Position = 0;
            using var reader = new StreamReader(
                context.Request.Body,
                Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                leaveOpen: true);

            requestBody = await reader.ReadToEndAsync();
            context.Request.Body.Position = 0;
        }

        try
        {
            await _next(context);

            if (context.Response.StatusCode >= 400)
            {
                _logger.LogError(
                    "HTTP {StatusCode} {Method} {Path}. Body: {Body}",
                    context.Response.StatusCode,
                    context.Request.Method,
                    context.Request.Path,
                    requestBody
                );
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Unhandled exception. {Method} {Path}. Body: {Body}",
                context.Request.Method,
                context.Request.Path,
                requestBody
            );

            throw;
        }
    }
}
