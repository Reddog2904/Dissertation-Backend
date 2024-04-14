using System;
using System.IO;
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
using Microsoft.AspNetCore.JsonPatch.Operations;

public static class PopulatePERatios
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string alphaVantageBaseFunctionUrl = "http://localhost:7144/api/AlphaVantageInterface";
    private static readonly string sqlBaseFunctionUrl = "http://localhost:7144/api/SQLInterface";

    [FunctionName("PopulatePERatios")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
        ILogger log)
    {
        // Retrieve the stock symbol from the HTTP request query.
        string symbol = req.Query["symbol"];

        string apiKey = Environment.GetEnvironmentVariable("AlphaVantageApiKey");

        // Validate that the symbol is provided.
        if (string.IsNullOrWhiteSpace(symbol))
        {
            return new BadRequestObjectResult("Please provide a stock symbol.");
        }

        // Fetch the Company ID associated with the symbol.
        int companyId = await FetchCompanyId(symbol, log);

        // Handle cases where the Company ID is not found.
        if (companyId == -1)
        {
            return new BadRequestObjectResult($"CompanyID for symbol {symbol} could not be found.");
        }

        // Fetch adjusted time series data for the given symbol.
        var adjustedTimeSeriesData = await FetchDataFromAlphaVantageInterface(symbol, "timeSeries", apiKey, log);

        // Check if the data fetch was unsuccessful.
        if (adjustedTimeSeriesData == null)
        {
            return new BadRequestObjectResult($"Failed to fetch adjusted time series data for symbol {symbol}.");
        }

        // Initialise a flag to track the success of all database inserts.
        bool allInsertsSuccessful = true;
        foreach (var date in adjustedTimeSeriesData)
        {
            string reportDate = date.Key;

            // Fetch P/E Ratio using CalculatePERatio function.
            decimal peRatio = await FetchAndParsePERatio(symbol, reportDate, log);

            // Check if the P/E ratio is valid.
            if (peRatio <= 0)
            {
                log.LogError($"Failed to fetch P/E Ratio for symbol {symbol} on {reportDate} or P/E Ratio is not positive.");
                allInsertsSuccessful = false;
                continue; // Skip to next iteration if there's an error.
            }

            // Insert P/E Ratio data into the database.
            bool insertSuccess = await InsertPERatioData(companyId, reportDate, peRatio, log);
            if (!insertSuccess)
            {
                log.LogError($"Failed to insert P/E Ratio data for symbol {symbol} on {reportDate}.");
                allInsertsSuccessful = false;
            }
        }

        // Return an error response if not all inserts were successful.
        if (!allInsertsSuccessful)
        {
            return new BadRequestObjectResult($"Failed to populate P/E Ratios for symbol {symbol}.");
        }

        // Return a success response if all operations were successful.
        return new OkObjectResult($"Successfully populated P/E Ratios for symbol {symbol}.");
    }

    // Fetches and parses the P/E ratio for a given stock symbol and date using the CalculatePERatio function.
    private static async Task<decimal> FetchAndParsePERatio(string symbol, string reportDate, ILogger log)
    {
        // URL for the CalculatePERatio function.
        string calculatePeRatioFunctionUrl = "http://localhost:7144/api/CalculatePERatio";
        string requestUrl = $"{calculatePeRatioFunctionUrl}?symbol={symbol}&date={reportDate}";

        log.LogInformation($"Requesting P/E Ratio for {symbol} on {reportDate} with URL: {requestUrl}");

        // Make the HTTP GET request to the CalculatePERatio function.
        HttpResponseMessage response = await client.GetAsync(requestUrl);

        // Read the response content as a string.
        string responseContent = await response.Content.ReadAsStringAsync();

        // Handle unsuccessful HTTP response.
        if (!response.IsSuccessStatusCode)
        {
            log.LogError($"Error calling CalculatePERatio for {symbol} on {reportDate}: {response.StatusCode}");
            log.LogError($"Response content: {responseContent}");
            return -1;
        }

        // Parse the JSON response content.
        JObject calculatePeData = JObject.Parse(responseContent);

        // Check for the presence of the 'PriceEarningsRatio' field in JSON.
        if (!calculatePeData.ContainsKey("PriceEarningsRatio"))
        {
            log.LogError($"PriceEarningsRatio not found in response content.");
            return -1;
        }

        // Extract the P/E Ratio value and return it.
        decimal peRatio = calculatePeData["PriceEarningsRatio"].Value<decimal>();
        return peRatio;
    }


    // Retrieves the CompanyID from the database for a given stock symbol.
    private static async Task<int> FetchCompanyId(string symbol, ILogger log)
    {
        // SQL query to retrieve CompanyID based on the stock symbol.
        string sql = "SELECT CompanyID FROM dbo.Companies WHERE Symbol = @Symbol";

        // Prepare payload with the SQL query and parameters for the HTTP request.
        var payload = new
        {
            sql,
            parameters = new { Symbol = symbol }
        };

        try
        {
            // Send a POST request to the SQL database interface with the payload.
            HttpResponseMessage response = await client.PostAsync(
                sqlBaseFunctionUrl,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));

            // Check if the HTTP response was successful.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error fetching CompanyID: {response.StatusCode}");
                return -1;
            }

            // Read and deserialise the JSON response into a list of dictionaries.
            string content = await response.Content.ReadAsStringAsync();
            var data = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(content);

            // Check if any data was returned and handle cases where the company ID is not found.
            if (data == null || data.Count == 0)
            {
                log.LogError("CompanyID not found for symbol: " + symbol);
                return -1;
            }

            // Convert and return the CompanyID as an integer.
            return Convert.ToInt32(data[0]["CompanyID"]);
        }
        catch (Exception e)
        {
            // Handle exceptions from the HTTP request or processing steps.
            log.LogError($"Exception fetching CompanyID: {e.Message}");
            return -1;
        }
    }

    // Fetches the Price to Earnings (P/E) ratios data from AlphaVantage for a given stock symbol.
    private static async Task<JObject> FetchPERatiosData(string symbol, ILogger log)
    {

        // Build the request URL for the AlphaVantage API with the symbol.
        string requestUrl = $"{alphaVantageBaseFunctionUrl}?operation=peRatio&symbol={symbol}";

        try
        {
            // Execute the HTTP GET request to the AlphaVantage API.
            HttpResponseMessage response = await client.GetAsync(requestUrl);

            // Check if the HTTP response was successful.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error calling AlphaVantageInterface for PE Ratios: {response.StatusCode}");
                return null;
            }

            // Read the response content as a string.
            string responseContent = await response.Content.ReadAsStringAsync();

            // Parse the JSON response content into a JObject.
            JObject peRatiosData = JObject.Parse(responseContent);

            // Return the parsed JSON object.
            return peRatiosData;
        }
        catch (Exception e)
        {
            log.LogError($"Exception when calling AlphaVantageInterface for PE Ratios: {e.Message}");
            return null;
        }
    }

    // Inserts P/E ratio data into the database for a given company ID and date.
    private static async Task<bool> InsertPERatioData(int companyId, string reportDate, decimal peRatio, ILogger log)
    {
        // SQL command for inserting P/E ratio into the database.
        string sql = @"
            INSERT INTO dbo.PERatios (CompanyID, DateOfRecord, Value) 
            VALUES (@CompanyID, @DateOfRecord, @Value)";

        // Prepare payload with the SQL command and parameters.
        var payload = new
        {
            sql,
            parameters = new
            {
                CompanyID = companyId,
                DateOfRecord = DateTime.ParseExact(reportDate, "yyyy-MM-dd", CultureInfo.InvariantCulture),
                Value = peRatio
            }
        };

        try
        {
            // Send a POST request to the SQL interface with the payload.
            HttpResponseMessage response = await client.PostAsync(
                sqlBaseFunctionUrl,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));

            // Check if the POST request was successful.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error inserting PE Ratio data: {response.StatusCode}");
                return false;
            }
            return true;
        }
        catch (Exception e)
        {
            log.LogError($"Exception inserting PE Ratio data: {e.Message}");
            return false;
        }
    }

    // Fetches data from AlphaVantage API based on the specified operation and symbol.
    private static async Task<JObject> FetchDataFromAlphaVantageInterface(string symbol, string operation, string apiKey, ILogger log)
    {
        // Construct the request URL with operation details, symbol, and API key.
        string requestUrl = $"{alphaVantageBaseFunctionUrl}?operation={operation}&symbol={symbol}&apikey={apiKey}";

        try
        {
            // Send a GET request to the AlphaVantage API.
            HttpResponseMessage response = await client.GetAsync(requestUrl);

            // Check if the HTTP response indicates success.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error calling AlphaVantageInterface for {operation}: {response.StatusCode}");
                return null;
            }

            // Read the API response content as a string.
            string responseContent = await response.Content.ReadAsStringAsync();

            // Parse the JSON response content into a JObject.
            JObject data = JObject.Parse(responseContent);

            // Return the parsed JSON object containing the data.
            return data;
        }
        catch (Exception e)
        {
            log.LogError($"Exception when calling AlphaVantageInterface for {operation}: {e.Message}");
            return null;
        }
    }

    // Retrieves the company overview data for a specified stock symbol using the AlphaVantage API.
    private static async Task<JObject> FetchCompanyOverviewData(string symbol, string apiKey, ILogger log)
    {
        // Set the API function to 'OVERVIEW' and build the request URL.
        string function = "OVERVIEW";
        string requestUrl = $"{alphaVantageBaseFunctionUrl}?function={function}&symbol={symbol}&apikey={apiKey}";

        try
        {
            // Perform a GET request to the AlphaVantage API to retrieve company overview.
            HttpResponseMessage response = await client.GetAsync(requestUrl);

            // Check if the API response was successful.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error calling AlphaVantage for company overview: {response.StatusCode}");
                return null;
            }

            // Read the API response content as a string.
            string responseContent = await response.Content.ReadAsStringAsync();

            // Parse the JSON string into a JObject to extract data.
            JObject companyOverviewData = JObject.Parse(responseContent);

            // Return the parsed data as a JObject.
            return companyOverviewData;
        }
        catch (Exception e)
        {
            log.LogError($"Exception when calling AlphaVantage for company overview: {e.Message}");
            return null;
        }
    }
}