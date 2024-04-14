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

public static class PopulateEarnings
{
    private static readonly HttpClient client = new HttpClient();
    private static readonly string alphaVantageBaseFunctionUrl = "http://localhost:7144/api/AlphaVantageInterface";
    private static readonly string sqlBaseFunctionUrl = "http://localhost:7144/api/SQLInterface";

    [FunctionName("PopulateEarnings")]
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

        // Call AlphaVantageInterface to get earnings data
        var earningsData = await FetchEarningsData(symbol, log);
        if (earningsData == null)
        {
            return new BadRequestObjectResult($"Failed to fetch earnings data for symbol {symbol}.");
        }

        // Parse and insert the earnings data into the dbo.Earnings table
        // Assume that earningsData is a JObject similar to time series data
        foreach (var item in earningsData)
        {
            string reportDate = item.Key; // The date of the earnings report
            decimal reportedEarnings = item.Value.Value<decimal>(); // The reported earnings

            // Construct and execute the SQL INSERT command to populate earnings
            bool insertSuccess = await InsertEarningsData(companyId, reportDate, reportedEarnings, log);
            if (!insertSuccess)
            {
                return new BadRequestObjectResult($"Failed to insert earnings data for symbol {symbol} on {reportDate}.");
            }
        }

        return new OkObjectResult($"Successfully populated earnings for symbol {symbol}.");
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
    private static async Task<JObject> FetchEarningsData(string symbol, ILogger log)
    {
        string requestUrl = $"{alphaVantageBaseFunctionUrl}?operation=earnings&symbol={symbol}";

        try
        {
            HttpResponseMessage response = await client.GetAsync(requestUrl);
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error calling AlphaVantageInterface for earnings: {response.StatusCode}");
                return null;
            }

            string responseContent = await response.Content.ReadAsStringAsync();
            JObject earningsData = JObject.Parse(responseContent); // Parse the JSON response
            return earningsData;
        }
        catch (Exception e)
        {
            log.LogError($"Exception when calling AlphaVantageInterface for earnings: {e.Message}");
            return null;
        }
    }

    private static async Task<bool> InsertEarningsData(int companyId, string reportDate, decimal reportedEarnings, ILogger log)
    {
        string sql = @"
            INSERT INTO dbo.Earnings (CompanyID, ReportDate, ReportedEarnings) 
            VALUES (@CompanyID, @ReportDate, @ReportedEarnings)";

        var payload = new
        {
            sql,
            parameters = new
            {
                CompanyID = companyId,
                ReportDate = DateTime.ParseExact(reportDate, "yyyy-MM-dd", CultureInfo.InvariantCulture),
                ReportedEarnings = reportedEarnings
            }
        };

        try
        {
            HttpResponseMessage response = await client.PostAsync(
                sqlBaseFunctionUrl,
                new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json"));

            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Error inserting earnings data: {response.StatusCode}");
                return false;
            }
            return true;
        }
        catch (Exception e)
        {
            log.LogError($"Exception inserting earnings data: {e.Message}");
            return false;
        }
    }
}