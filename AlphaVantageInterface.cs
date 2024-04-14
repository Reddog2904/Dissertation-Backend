using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

public static class AlphaVantageInterface
{
    private static readonly HttpClient client = new HttpClient();

    [FunctionName("AlphaVantageInterface")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
        ILogger log)
    {
        // Retrieve 'operation' and 'symbol' from query parameters.
        string operation = req.Query["operation"]; // "overview", "timeSeries", or "earnings".
        string symbol = req.Query["symbol"];

        // Validate presence of required query parameters.
        if (string.IsNullOrEmpty(symbol) || string.IsNullOrEmpty(operation))
        {
            log.LogWarning("AlphaVantageInterface function called without symbol or operation.");
            return new BadRequestObjectResult("Error: Please pass a stock symbol and operation on the query string.");
        }

        // API key for Alpha Vantage API.
        string apiKey = "Alpha-Vantage-API-Key";
        string requestUrl = string.Empty;

        // Log the operation and symbol.
        log.LogInformation($"Operation: {operation}, Symbol: {symbol}");

        // Determine API function based on 'operation'.
        // Overview
        if (operation.Equals("overview", StringComparison.OrdinalIgnoreCase))
        {
            requestUrl = $"https://www.alphavantage.co/query?function=OVERVIEW&symbol={symbol}&apikey={apiKey}";
        }

        // Time Series
        else if (operation.Equals("timeSeries", StringComparison.OrdinalIgnoreCase))
        {
            requestUrl = $"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={apiKey}&outputsize=compact";
        }

        // Earnings
        else if (operation.Equals("earnings", StringComparison.OrdinalIgnoreCase))
        {
            requestUrl = $"https://www.alphavantage.co/query?function=EARNINGS&symbol={symbol}&apikey={apiKey}";
        }

        // Handle invalid operation by returning error response.
        if (string.IsNullOrEmpty(requestUrl))
        {
            log.LogError("Invalid operation specified in AlphaVantageInterface function.");
            return new BadRequestObjectResult("Invalid operation specified.");
        }

        // Log the request URL.
        log.LogInformation($"Request URL: {requestUrl}");

        try
        {
            // Execute HTTP GET request to Alpha Vantage API.
            HttpResponseMessage response = await client.GetAsync(requestUrl);
            string responseBody = await response.Content.ReadAsStringAsync();

            // Log response status code.
            log.LogInformation($"Response StatusCode: {response.StatusCode}");

            // Handle unsuccessful response.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error calling Alpha Vantage API: {response.StatusCode}");
                return new StatusCodeResult((int)response.StatusCode);
            }

            // Parse JSON response and return result.
            JObject jsonData = JObject.Parse(responseBody);
            return new OkObjectResult(jsonData);
        }
        catch (HttpRequestException httpRequestEx)
        {
            log.LogError($"HTTP Request Exception: {httpRequestEx.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
        catch (JsonReaderException jsonReaderEx)
        {
            log.LogError($"JSON Reader Exception: {jsonReaderEx.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
        catch (Exception e)
        {
            log.LogError($"General Exception when calling Alpha Vantage API: {e.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }
}