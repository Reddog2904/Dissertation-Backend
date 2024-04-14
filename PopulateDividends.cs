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

public static class PopulateDividends
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string alphaVantageBaseFunctionUrl = "http://localhost:7144/api/AlphaVantageInterface";
    private static readonly string sqlBaseFunctionUrl = "http://localhost:7144/api/SQLInterface";

    [FunctionName("PopulateDividends")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
        ILogger log)
    {
        string symbol = req.Query["symbol"];
        if (string.IsNullOrWhiteSpace(symbol))
        {
            return new BadRequestObjectResult("Please provide a stock symbol.");
        }

        // Fetch CompanyID based on the symbol
        int companyId = await FetchCompanyId(symbol, log);
        if (companyId == -1)
        {
            return new BadRequestObjectResult($"CompanyID for symbol {symbol} could not be found.");
        }

        // Call AlphaVantageInterface to get dividend data
        var dividendData = await FetchDividendData(symbol, log);
        if (dividendData == null)
        {
            return new BadRequestObjectResult($"Failed to fetch dividend data for symbol {symbol}.");
        }

        // Parse and insert the dividend data into the dbo.Dividends table
        // Assume that dividendData is a JObject similar to time series data
        foreach (var item in dividendData)
        {
            string recordDate = item.Key; // The date of the dividend
            decimal dividendAmount = item.Value.Value<decimal>(); // The amount of the dividend

            // Construct and execute the SQL INSERT command to populate dividends
            bool insertSuccess = await InsertDividendData(companyId, recordDate, dividendAmount, log);
            if (!insertSuccess)
            {
                return new BadRequestObjectResult($"Failed to insert dividend data for symbol {symbol} on {recordDate}.");
            }
        }

        return new OkObjectResult($"Successfully populated dividends for symbol {symbol}.");
    }

    private static async Task<int> FetchCompanyId(string symbol, ILogger log)
    {
        string sql = "SELECT CompanyID FROM dbo.Companies WHERE Symbol = @Symbol";
        var payload = new
        {
            sql,
            parameters = new { Symbol = symbol }
        };

        try
        {
            HttpResponseMessage response = await client.PostAsync(
                sqlBaseFunctionUrl,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));

            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error fetching CompanyID: {response.StatusCode}");
                return -1;
            }

            string content = await response.Content.ReadAsStringAsync();
            var data = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(content);
            if (data == null || data.Count == 0)
            {
                log.LogError("CompanyID not found for symbol: " + symbol);
                return -1;
            }

            return Convert.ToInt32(data[0]["CompanyID"]);
        }
        catch (Exception e)
        {
            log.LogError($"Exception fetching CompanyID: {e.Message}");
            return -1;
        }
    }
    private static async Task<JObject> FetchDividendData(string symbol, ILogger log)
    {
        string requestUrl = $"{alphaVantageBaseFunctionUrl}?operation=dividends&symbol={symbol}";

        try
        {
            HttpResponseMessage response = await client.GetAsync(requestUrl);
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error calling AlphaVantageInterface for dividends: {response.StatusCode}");
                return null;
            }

            string responseContent = await response.Content.ReadAsStringAsync();
            JObject dividendData = JObject.Parse(responseContent); // Parse the JSON response
            return dividendData;
        }
        catch (Exception e)
        {
            log.LogError($"Exception when calling AlphaVantageInterface for dividends: {e.Message}");
            return null;
        }
    }

    private static async Task<bool> InsertDividendData(int companyId, string recordDate, decimal dividendAmount, ILogger log)
    {
        string sql = @"
            INSERT INTO dbo.Dividends (CompanyID, Date, DividendAmount) 
            VALUES (@CompanyID, @Date, @DividendAmount)";

        var payload = new
        {
            sql,
            parameters = new
            {
                CompanyID = companyId,
                Date = DateTime.ParseExact(recordDate, "yyyy-MM-dd", CultureInfo.InvariantCulture),
                DividendAmount = dividendAmount
            }
        };

        try
        {
            HttpResponseMessage response = await client.PostAsync(
                sqlBaseFunctionUrl,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));

            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error inserting dividend data: {response.StatusCode}");
                return false;
            }
            return true;
        }
        catch (Exception e)
        {
            log.LogError($"Exception inserting dividend data: {e.Message}");
            return false;
        }
    }
}
