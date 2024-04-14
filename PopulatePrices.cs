using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Globalization;
using System.Text;
using System.Collections.Generic;

public static class PopulatePrices
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string alphaVantageBaseFunctionUrl = "http://localhost:7144/api/AlphaVantageInterface";
    private static readonly string sqlBaseFunctionUrl = "http://localhost:7144/api/SQLInterface";

    [FunctionName("PopulatePrices")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
        ILogger log)
    {

        // Extract 'symbol' and 'date' parameters from the HTTP request.
        string symbol = req.Query["symbol"];
        string date = req.Query["date"];

        // Check if 'symbol' or 'date' are missing or empty.
        if (string.IsNullOrWhiteSpace(symbol) || string.IsNullOrWhiteSpace(date))
        {
            return new BadRequestObjectResult("Please provide both a stock symbol and a date.");
        }

        // Validate the date format
        if (!DateTime.TryParseExact(date, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime parsedDate))
        {
            return new BadRequestObjectResult("Invalid date format. Please use YYYY-MM-DD format.");
        }

        // Retrieve the company ID from the database based on the symbol.
        int companyId = await FetchCompanyId(symbol, log);
        if (companyId == -1)
        {
            return new BadRequestObjectResult($"CompanyID for symbol {symbol} could not be found.");
        }

        // Fetch time series data from AlphaVantage for the given symbol.
        var timeSeriesData = await FetchTimeSeriesData(symbol, log);
        if (timeSeriesData == null)
        {
            return new BadRequestObjectResult($"Failed to fetch time series data for symbol {symbol}.");
        }

        // Locate the price data for the specified date in the time series.
        JObject priceData = timeSeriesData.GetValue(parsedDate.ToString("yyyy-MM-dd")) as JObject;
        if (priceData == null)
        {
            return new BadRequestObjectResult($"No price data found for symbol {symbol} on {date}.");
        }

        // Parse closing price from the price data.
        decimal closePrice = priceData["4. close"].Value<decimal>();

        // Insert the closing price into the database for the specified date.
        bool insertSuccess = await InsertPriceData(companyId, date, closePrice, log);
        if (!insertSuccess)
        {
            return new BadRequestObjectResult($"Failed to insert price data for symbol {symbol} on {date}.");
        }

        // Return success message if price data was successfully inserted.
        return new OkObjectResult($"Successfully populated prices for symbol {symbol} on {date}.");
    }

    // Fetches the CompanyID for a given stock symbol from the database.
    private static async Task<int> FetchCompanyId(string symbol, ILogger log)
    {
        // SQL query to select CompanyID for a given symbol.
        string sql = "SELECT CompanyID FROM dbo.Companies WHERE Symbol = @Symbol";
        var payload = new
        {
            sql,
            parameters = new { Symbol = symbol }
        };

        try
        {
            // Send SQL query to the database through the SQL interface.
            HttpResponseMessage response = await client.PostAsync(
                sqlBaseFunctionUrl,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));

            // Check if the HTTP response was successful.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error fetching CompanyID: {response.StatusCode}");
                return -1;
            }

            // Parse the HTTP response content.
            string content = await response.Content.ReadAsStringAsync();
            var data = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(content);

            // Ensure data was received and contains the expected information.
            if (data == null || data.Count == 0)
            {
                log.LogError("CompanyID not found for symbol: " + symbol);
                return -1;
            }

            // Return the CompanyID from the fetched data.
            return Convert.ToInt32(data[0]["CompanyID"]);
        }
        catch (Exception e)
        {
            log.LogError($"Exception fetching CompanyID: {e.Message}");
            return -1;
        }
    }

    // Retrieves the time series data for a specified stock symbol using the AlphaVantage API.
    private static async Task<JObject> FetchTimeSeriesData(string symbol, ILogger log)
    {

        // Construct the request URL with the required query parameters.
        string requestUrl = $"{alphaVantageBaseFunctionUrl}?operation=timeSeries&symbol={symbol}";

        try
        {
            // Execute the HTTP GET request to the specified URL.
            HttpResponseMessage response = await client.GetAsync(requestUrl);

            // Check if the response status code indicates a failure.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error calling AlphaVantageInterface for time series: {response.StatusCode}");
                return null;
            }

            // Read and parse the JSON content from the HTTP response.
            string responseContent = await response.Content.ReadAsStringAsync();
            JObject timeSeriesData = JObject.Parse(responseContent)["Time Series (Daily)"] as JObject;
            return timeSeriesData;
        }
        catch (Exception e)
        {
            log.LogError($"Exception when calling AlphaVantageInterface for time series: {e.Message}");
            return null;
        }
    }

    // Inserts price data into the database for a given company and date.
    private static async Task<bool> InsertPriceData(int companyId, string dateString, decimal pricePerShare, ILogger log)
    {
        // SQL command to insert price data into the Prices table.
        string sql = @"
            INSERT INTO dbo.Prices (CompanyID, Date, PricePerShare) 
            VALUES (@CompanyID, @Date, @PricePerShare)";

        // Parameters for the SQL query, ensuring the date format is correct.
        var payload = new
        {
            sql,
            parameters = new
            {
                CompanyID = companyId,
                Date = DateTime.ParseExact(dateString, "yyyy-MM-dd", CultureInfo.InvariantCulture),
                PricePerShare = pricePerShare
            }
        };

        try
        {
            // Send the SQL data to the server using a POST request.
            HttpResponseMessage response = await client.PostAsync(
                sqlBaseFunctionUrl,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));

            // Check if the response indicates a failure.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error inserting price data: {response.StatusCode}");
                return false;
            }
            return true;
        }
        catch (Exception e)
        {
            log.LogError($"Exception inserting price data: {e.Message}");
            return false;
        }
    }
}
