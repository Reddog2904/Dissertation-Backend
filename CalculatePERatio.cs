using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System;
using Newtonsoft.Json;
using System.Globalization;

public static class CalculatePERatioFunction
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string alphaVantageInterfaceUrl = "http://localhost:7144/api/AlphaVantageInterface";

    [FunctionName("CalculatePERatio")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
        ILogger log)
    {
        log.LogInformation("CalculatePERatio function started processing a request.");

        // Retrieves query parameters.
        string symbol = req.Query["symbol"];
        string date = req.Query["date"];

        // Checks if the required query parameters are provided.
        if (string.IsNullOrEmpty(symbol) || string.IsNullOrEmpty(date))
        {
            log.LogWarning("Symbol or date parameters are missing.");
            return new BadRequestObjectResult("Please provide a stock symbol and a date in the query string.");
        }

        try
        {
            // Fetches company overview data from the Alpha Vantage API.
            string overviewResponseContent = await GetAlphaVantageData("overview", symbol, log);
            if (string.IsNullOrEmpty(overviewResponseContent))
            {
                log.LogError("Overview data is empty.");
                return new BadRequestObjectResult("Overview data could not be retrieved.");
            }

            // Parses JSON data from the overview response.
            JObject overviewData = JObject.Parse(overviewResponseContent);
            decimal eps;

            // Parses EPS (Earnings Per Share) from the overview data.
            string epsString = overviewData["EPS"]?.ToString().Replace(",", string.Empty);
            if (!decimal.TryParse(epsString, NumberStyles.Any, CultureInfo.InvariantCulture, out eps) || eps <= 0)
            {
                log.LogError($"EPS format exception. Value: {epsString}");
                return new BadRequestObjectResult("EPS format is invalid.");
            }

            // Fetches time series data from Alpha Vantage API.
            string timeSeriesResponseContent = await GetAlphaVantageData("timeSeries", symbol, log);
            if (string.IsNullOrEmpty(timeSeriesResponseContent))
            {
                log.LogError("Time series data is empty.");
                return new BadRequestObjectResult("Time series data could not be retrieved.");
            }

            // Parses JSON data for the specific date from the time series response.
            JObject timeSeriesData = JObject.Parse(timeSeriesResponseContent);
            JObject timeSeriesForDate = timeSeriesData["Time Series (Daily)"]?[date]?.Value<JObject>();
            if (timeSeriesForDate == null)
            {
                log.LogError("The data for the provided date is not available.");
                return new BadRequestObjectResult("The data for the provided date is not available.");
            }

            // Parses the latest close price from the time series data.
            decimal latestClosePrice;
            string latestClosePriceString = timeSeriesForDate["4. close"]?.ToString().Replace(",", string.Empty);
            if (!decimal.TryParse(latestClosePriceString, NumberStyles.Any, CultureInfo.InvariantCulture, out latestClosePrice) || latestClosePrice <= 0)
            {
                log.LogError($"Latest close price format exception. Value: {latestClosePriceString}");
                return new BadRequestObjectResult("Latest close price format is invalid.");
            }

            // Calculates the Price to Earnings ratio and rounds to two decimal places.
            decimal peRatio = Math.Round(latestClosePrice / eps, 2);

            // Constructs the result object to return.
            var result = new
            {
                Symbol = symbol,
                Date = date,
                LatestClosePrice = latestClosePrice,
                EarningsPerShare = eps,
                PriceEarningsRatio = peRatio
            };

            // Logs the successful completion of the function.
            log.LogInformation("CalculatePERatio function completed successfully.");
            return new OkObjectResult(result);
        }
        catch (Exception e)
        {
            // Logs the exception details if an error occurs.
            log.LogError($"Exception encountered: {e.Message}");
            log.LogError($"Exception stack trace: {e.StackTrace}");
            if (e.InnerException != null)
            {
                log.LogError($"Inner exception: {e.InnerException.Message}");
                log.LogError($"Inner exception stack trace: {e.InnerException.StackTrace}");
            }
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }


    private static async Task<string> GetAlphaVantageData(string operation, string symbol, ILogger log)
    {
        // Constructs the request URL for the Alpha Vantage API.
        string requestUrl = $"{alphaVantageInterfaceUrl}?operation={operation}&symbol={symbol}";
        try
        {
            // Executes the HTTP GET request.
            HttpResponseMessage response = await client.GetAsync(requestUrl);

            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"AlphaVantageInterface call for {operation} failed with status code: {response.StatusCode}");
                log.LogError($"Response content: {await response.Content.ReadAsStringAsync()}");
                return null;
            }

            // Reads the response content as a string.
            string content = await response.Content.ReadAsStringAsync();

            // Checks if the content returned is empty.
            if (string.IsNullOrWhiteSpace(content))
            {
                log.LogError($"AlphaVantageInterface call for {operation} returned empty content.");
                return null;
            }

            // Attempt to parse the JSON content to ensure it is valid.
            try
            {
                var data = JObject.Parse(content);
                return content;
            }
            catch (JsonReaderException jsonEx)
            {
                log.LogError($"Failed to parse JSON content: {jsonEx.Message}");
                log.LogError($"JSON content: {content}");
                return null;
            }
        }

        // Handle exceptions related to the HTTP request.
        catch (HttpRequestException httpEx)
        {
            log.LogError($"HTTP request exception: {httpEx.Message}");
            return null;
        }

        // Catches and logs any other exceptions that may occur.
        catch (Exception ex)
        {
            log.LogError($"Exception encountered: {ex.Message}");
            log.LogError($"Exception stack trace: {ex.StackTrace}");
            if (ex.InnerException != null)
            {
                log.LogError($"Inner exception: {ex.InnerException.Message}");
                log.LogError($"Inner exception stack trace: {ex.InnerException.StackTrace}");
            }
            return null;
        }
    }
}