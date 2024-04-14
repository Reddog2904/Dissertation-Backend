using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Globalization;
using System.Net.Http;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Text;
using System.Linq;
using System.Collections;

public static class PopulateDatabaseFunction
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string baseFunctionUrl = "http://localhost:7144/api/";

    [FunctionName("PopulateDatabaseFunction")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
        ILogger log)
    {
        log.LogInformation("C# HTTP trigger function processed a request.");

        // Retrieve start and end dates from the query parameters.
        string startDateStr = req.Query["startDate"];
        string endDateStr = req.Query["endDate"];

        // Validate and parse the start and end date parameters.
        if (!DateTime.TryParse(startDateStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime startDate) ||
            !DateTime.TryParse(endDateStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime endDate))
        {
            return new BadRequestObjectResult("Invalid startDate or endDate. Please use YYYY-MM-DD format.");
        }

        // Fetch all symbols from the database.
        List<string> symbols = await FetchAllSymbols(log);
        if (symbols == null || symbols.Count == 0)
        {
            return new BadRequestObjectResult("Failed to fetch symbols or no symbols found.");
        }

        // Process each symbol to populate the database.
        foreach (var symbol in symbols)
        {
            await ProcessSymbol(symbol, startDate, endDate, log);
        }

        // Return a success result after all symbols have been processed.
        return new OkObjectResult("Database population completed successfully for all symbols.");
    }

    // Processes a single symbol by fetching the company ID, populating P/E ratios, and populating prices for a date range.
    private static async Task ProcessSymbol(string symbol, DateTime startDate, DateTime endDate, ILogger log)
    {
        // Attempt to retrieve the company ID for the given stock symbol.
        int companyId = await FetchCompanyId(symbol, log);
        if (companyId < 0)
        {
            log.LogError($"CompanyID for the specified symbol {symbol} could not be found.");
            return; // Exit if no company ID is found to avoid further processing.
        }

        // Populate P/E Ratios for the given symbol and date range.
        bool peratiosSuccess = await PopulatePERatios(companyId, symbol, startDate, endDate, log);
        if (!peratiosSuccess)
        {
            log.LogError($"Failed to populate P/E Ratios for symbol {symbol}.");
        }

        // Iterate over each date in the range and attempt to populate daily price data.
        foreach (var date in GenerateDateRange(startDate, endDate))
        {
            bool dailyPriceSuccess = await CallFunction("PopulatePrices", symbol, date.ToString("yyyy-MM-dd"), date.ToString("yyyy-MM-dd"), log);
            if (!dailyPriceSuccess)
            {
                log.LogWarning($"No price data for {date.ToString("yyyy-MM-dd")} or failed to populate Prices for {date.ToString("yyyy-MM-dd")}.");
            }
        }
    }

    // Retrieves a list of all stock symbols from the database via an HTTP POST request.
    private static async Task<List<string>> FetchAllSymbols(ILogger log)
    {
        // Define the request URL and payload for the SQL query.
        string url = $"{baseFunctionUrl}SQLInterface";
        var payload = new
        {
            sql = "SELECT Symbol FROM dbo.Companies",
            parameters = new Dictionary<string, object>()
        };

        try
        {
            // Execute the HTTP POST request with the SQL query.
            HttpResponseMessage response = await client.PostAsync(
                url,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));

            // Check the status code of the response.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error fetching symbols: {response.StatusCode}");
                return new List<string>(); // Return an empty list if the request was unsuccessful.
            }

            // Parse the JSON response to extract symbols.
            string content = await response.Content.ReadAsStringAsync();
            var data = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(content);
            if (data == null)
            {
                log.LogError("No symbols found.");
                return new List<string>(); // Return an empty list if no data is found.
            }

            // Convert the result to a list of strings containing symbols.
            List<string> symbols = data.Select(row => row["Symbol"].ToString()).ToList();
            return symbols;
        }
        catch (Exception ex)
        {
            // Log any exceptions that occur during the fetch operation.
            log.LogError($"Exception in FetchAllSymbols: {ex.Message}");
            return new List<string>(); // Return an empty list if an exception is caught.
        }
    }

    // Validates the input parameters for symbol, startDate and endDate.
    private static bool ValidateInputs(string symbol, string startDateStr, string endDateStr, out DateTime startDate, out DateTime endDate, out IActionResult validationResult)
    {
        validationResult = null;
        startDate = default;
        endDate = default;

        // Check if the symbol parameter is provided and non-empty.
        if (string.IsNullOrWhiteSpace(symbol))
        {
            validationResult = new BadRequestObjectResult("Please provide a symbol query parameter.");
            return false; // Return false to indicate invalid input.
        }

        // Try to parse the startDate and endDate from their string representations.
        if (!DateTime.TryParse(startDateStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out startDate) ||
            !DateTime.TryParse(endDateStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out endDate))
        {
            validationResult = new BadRequestObjectResult("Invalid startDate or endDate. Please use YYYY-MM-DD format.");
            return false; // Return false if either date fails to parse correctly.
        }

        // Return true if all inputs are valid.
        return true;
    }

    // Populates various financial data including P/E ratios, earnings, dividends, and prices for a given company and date range.
    private static async Task<bool> PopulateData(int companyId, string symbol, DateTime startDate, DateTime endDate, ILogger log)
    {
        // Populate P/E Ratios for the given symbol and date range.
        bool peratiosSuccess = await PopulatePERatios(companyId, symbol, startDate, endDate, log);
        if (!peratiosSuccess)
        {
            log.LogError("Failed to populate P/E Ratios.");
            return false;
        }

        // Populate earnings for the given symbol and date range.
        bool earningsSuccess = await CallFunction("PopulateEarnings", symbol, startDate.ToString("yyyy-MM-dd"), endDate.ToString("yyyy-MM-dd"), log);
        if (!earningsSuccess)
        {
            log.LogError("Failed to populate Earnings.");
            return false;
        }

        // Populate dividends for the given symbol and date range.
        bool dividendsSuccess = await CallFunction("PopulateDividends", symbol, startDate.ToString("yyyy-MM-dd"), endDate.ToString("yyyy-MM-dd"), log);
        if (!dividendsSuccess)
        {
            log.LogError("Failed to populate Dividends.");
            return false;
        }

        // Iterate over each date in the range to populate prices individually.
        foreach (var date in GenerateDateRange(startDate, endDate))
        {
            if (!await CallFunction("PopulatePrices", symbol, date.ToString("yyyy-MM-dd"), "", log))
            {
                log.LogError($"Failed to populate Prices for {date:yyyy-MM-dd}.");
                return false;
            }
        }

        // Return true if all data is populated successfully.
        return true;
    }

    // Helper method to generate a list of dates
    private static List<DateTime> GenerateDateRange(DateTime start, DateTime end)
    {
        List<DateTime> range = new List<DateTime>();
        // Loop through each day from the start date to the end date.
        for (DateTime date = start; date <= end; date = date.AddDays(1))
        {
            range.Add(date);
        }
        return range;
    }

    // Fetches and parses the Price-to-Earnings (P/E) ratio for a given stock symbol on a specific date.
    private static async Task<decimal> FetchAndParsePERatio(string symbol, DateTime date, ILogger log)
    {
        string dateString = date.ToString("yyyy-MM-dd");
        string url = $"http://localhost:7144/api/CalculatePERatio?symbol={symbol}&date={dateString}";

        try
        {
            // Send a GET request to the CalculatePERatio function.
            HttpResponseMessage response = await client.GetAsync(url);
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error calling CalculatePERatio for {dateString}: {response.StatusCode}");
                return -1;
            }

            // Read the response content.
            string content = await response.Content.ReadAsStringAsync();
            JObject jObject = JObject.Parse(content);

            // Extract and return the P/E ratio value.
            return jObject["priceEarningsRatio"].Value<decimal>();
        }
        catch (Exception ex)
        {
            log.LogError($"Exception in FetchAndParsePERatio: {ex.Message}");
            return -1;
        }
    }

    // Retrieves the CompanyID from the database for a given stock symbol using an SQL query.
    private static async Task<int> FetchCompanyId(string symbol, ILogger log)
    {
        // SQLInterface function interface for SQL queries.
        string url = $"http://localhost:7144/api/SQLInterface";
        var payload = new
        {
            sql = "SELECT CompanyID FROM dbo.Companies WHERE Symbol = @Symbol", // SQL query to fetch CompanyID.
            parameters = new { Symbol = symbol } // Parameter for the SQL query.
        };

        try
        {
            HttpResponseMessage response = await client.PostAsync(
                url,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json")); // Send the SQL query to the SQLInterface function.

            // Log errors if the request was unsuccessful.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error retrieving CompanyID: {response.StatusCode}");
                return -1;
            }

            // Deserialise the JSON response to a list of dictionaries.
            string content = await response.Content.ReadAsStringAsync();
            var data = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(content);

            // Log an error if no data is returned.
            if (data == null || data.Count == 0)
            {
                log.LogError("CompanyID not found.");
                return -1;
            }

            // Convert and return the CompanyID.
            return Convert.ToInt32(data[0]["CompanyID"]);
        }
        catch (Exception ex)
        {
            log.LogError($"Exception in FetchCompanyId: {ex.Message}");
            return -1;
        }
    }

    // Inserts a P/E ratio value into the database for a specified company and date.
    private static async Task<bool> InsertPERatio(int companyId, DateTime date, decimal peRatio, ILogger log)
    {
        string url = $"http://localhost:7144/api/SQLInterface";
        string dateString = date.ToString("yyyy-MM-dd");

        // SQL command to insert the P/E ratio.
        var payload = new
        {
            sql = "INSERT INTO dbo.PERatios (CompanyID, DateOfRecord, Value) VALUES (@CompanyID, @DateOfRecord, @Value)", 
            parameters = new
            {
                CompanyID = companyId,
                DateOfRecord = dateString,
                Value = peRatio
            }
        };

        try
        {
            // Send the SQL insert command to the SQLInterface function.
            HttpResponseMessage response = await client.PostAsync(
                url,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));

            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error inserting P/E Ratio for {dateString}: {response.StatusCode}");
                return false;
            }

            // Log success message.
            log.LogInformation($"Successfully inserted P/E Ratio for {dateString}.");
            return true;
        }
        catch (Exception ex)
        {
            // Log any exceptions.
            log.LogError($"Exception in InsertPERatio: {ex.Message}");
            return false;
        }
    }

    // Populates the P/E Ratios for a given company and symbol over a specified date range.
    private static async Task<bool> PopulatePERatios(int companyId, string symbol, DateTime startDate, DateTime endDate, ILogger log)
    {
        // Iterate over each date in the specified range.
        foreach (var date in GenerateDateRange(startDate, endDate))
        {

            // Fetch and parse the P/E ratio for the given symbol and date.
            decimal peRatio = await FetchAndParsePERatio(symbol, date, log);

            // Check if the fetched P/E ratio is valid.
            if (peRatio < 0)
            {
                log.LogWarning($"No P/E Ratio data available for {date:yyyy-MM-dd}.");
                continue; // Skip to the next date if data is not available for the current date.
            }

            // Attempt to insert the fetched P/E ratio into the database.
            bool success = await InsertPERatio(companyId, date, peRatio, log);
            if (!success)
            {
                log.LogError($"Failed to insert P/E Ratio for {date:yyyy-MM-dd}.");
                return false; // Return false if inserting P/E Ratio failed.
            }
        }

        // Return true if all P/E Ratios are inserted successfully or skipped due to no data.
        return true;
    }

    // Calls an external API function by name for a specified symbol and date, handling API rate limits.
    private static async Task<bool> CallFunction(string functionName, string symbol, string date, string dummyEndDate, ILogger log)
    {
        // Construct the URL for the API call including the function name, symbol, and date.
        string functionUrl = $"{baseFunctionUrl}{functionName}?symbol={symbol}&date={date}";

        try
        {
            // Make the GET request to the specified API function.
            HttpResponseMessage response = await client.GetAsync(functionUrl);
            await Task.Delay(2000); // Wait for 2 seconds after each call due to rate limits on the API.

            // Check if the call was successful.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Failed to call {functionName} for symbol {symbol} on {date}. Status: {response.StatusCode}");
                return false;
            }

            // Return true if the API call was successful.
            return true;
        }
        catch (Exception ex)
        {
            // Log any exceptions that occur during the API call.
            log.LogError($"Exception calling {functionName} for symbol {symbol} on {date}: {ex.Message}");
            return false;
        }
    }

    // Validates the provided input parameters for completeness and correct format, returning appropriate action results on failure.
    private static IActionResult ValidateInputs(string symbol, string startDateStr, string endDateStr)
    {
        if (string.IsNullOrWhiteSpace(symbol))
        {
            return new BadRequestObjectResult("Please provide a symbol query parameter.");
        }
        if (string.IsNullOrWhiteSpace(startDateStr))
        {
            return new BadRequestObjectResult("Please provide a startDate query parameter.");
        }
        if (string.IsNullOrWhiteSpace(endDateStr))
        {
            return new BadRequestObjectResult("Please provide an endDate query parameter.");
        }

        if (!DateTime.TryParse(startDateStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime startDate) ||
            !DateTime.TryParse(endDateStr, CultureInfo.InvariantCulture, DateTimeStyles.None, out DateTime endDate))
        {
            return new BadRequestObjectResult("Invalid startDate or endDate. Please use YYYY-MM-DD format.");
        }

        return null; // No errors
    }

    // Evaluates the success of various data population operations and returns an appropriate IActionResult based on the outcomes.
    private static IActionResult CheckSuccess(bool peratiosSuccess, bool earningsSuccess, bool dividendsSuccess, bool pricesSuccess)
    {
        if (!peratiosSuccess)
        {
            return new BadRequestObjectResult("Failed to populate P/E Ratios.");
        }
        if (!earningsSuccess)
        {
            return new BadRequestObjectResult("Failed to populate Earnings.");
        }
        if (!dividendsSuccess)
        {
            return new BadRequestObjectResult("Failed to populate Dividends.");
        }
        if (!pricesSuccess)
        {
            return new BadRequestObjectResult("Failed to populate Prices.");
        }

        return new OkObjectResult("Database population completed successfully.");
    }

}