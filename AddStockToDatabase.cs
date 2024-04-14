using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.IO;

public static class AddStockToDatabase
{
    private static readonly HttpClient client = new HttpClient();

    [FunctionName("AddStockToDatabase")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
        ILogger log)
    {
        log.LogInformation("C# HTTP trigger function processed a request to add a stock symbol.");

        // Read the entire request body into a string.
        string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
        JObject data;
        try
        {
            data = JObject.Parse(requestBody);
        }
        catch (JsonReaderException jex)
        {
            log.LogError("Parsing error on input: " + jex.Message);
            return new BadRequestObjectResult("Invalid JSON format received.");
        }

        // Retrieve the 'ticker' value from the parsed JSON.
        string ticker = data["ticker"]?.Value<string>();
        if (string.IsNullOrWhiteSpace(ticker))
        {
            log.LogError("The stock symbol is missing in the request body.");
            return new BadRequestObjectResult("Please pass a stock symbol in the request body.");
        }

        // Prepare to call the Alpha Vantage API with the 'overview' operation.
        string operation = "overview";
        var alphaVantageResponse = await client.GetAsync($"http://localhost:7144/api/AlphaVantageInterface?symbol={ticker}&operation={operation}");

        if (!alphaVantageResponse.IsSuccessStatusCode)
        {
            // Log and handle HTTP errors from the Alpha Vantage API.
            log.LogError($"Error retrieving company details: {alphaVantageResponse.StatusCode}");
            return new StatusCodeResult((int)alphaVantageResponse.StatusCode);
        }

        // Parse the JSON response from Alpha Vantage.
        string responseBody = await alphaVantageResponse.Content.ReadAsStringAsync();
        JObject alphaVantageData;
        try
        {
            alphaVantageData = JObject.Parse(responseBody);
        }
        catch (JsonReaderException jex)
        {
            log.LogError("Error parsing JSON from Alpha Vantage response: " + jex.Message);
            return new BadRequestObjectResult("Error parsing JSON from Alpha Vantage response.");
        }

        // Extract company details from the Alpha Vantage JSON response.
        string companyName = alphaVantageData["Name"]?.Value<string>();
        string sector = alphaVantageData["Sector"]?.Value<string>();
        string industry = alphaVantageData["Industry"]?.Value<string>();

        // Log and return an error if any key company details are missing.
        if (string.IsNullOrWhiteSpace(companyName))
        {
            log.LogError("Company Name is missing from Alpha Vantage response.");
            return new BadRequestObjectResult("Company Name is missing from Alpha Vantage response.");
        }
        if (string.IsNullOrWhiteSpace(sector))
        {
            log.LogError("Sector is missing from Alpha Vantage response.");
            return new BadRequestObjectResult("Sector is missing from Alpha Vantage response.");
        }
        if (string.IsNullOrWhiteSpace(industry))
        {
            log.LogError("Industry is missing from Alpha Vantage response.");
            return new BadRequestObjectResult("Industry is missing from Alpha Vantage response.");
        }

        // Prepare SQL command and parameters for database insertion.
        string sqlCommand = "INSERT INTO dbo.Companies (Symbol, Name, Sector, Industry) VALUES (@symbol, @name, @sector, @industry);";
        var sqlData = new
        {
            sql = sqlCommand,
            parameters = new { symbol = ticker, name = companyName, sector = sector, industry = industry }
        };
        var sqlDataJson = JsonConvert.SerializeObject(sqlData);
        var content = new StringContent(sqlDataJson, Encoding.UTF8, "application/json");

        // Post data to the SQL Interface for database insertion.
        var sqlInterfaceUrl = "http://localhost:7144/api/SQLInterface";
        var response = await client.PostAsync(sqlInterfaceUrl, content);
        if (!response.IsSuccessStatusCode)
        {
            // Log and handle errors from the SQL Interface.
            string errorContent = await response.Content.ReadAsStringAsync();
            log.LogError($"SQL Interface returned an error: {errorContent}");
            return new StatusCodeResult((int)response.StatusCode);
        }

        // Return success result after adding stock details to the database.
        return new OkObjectResult($"Stock symbol {ticker} with company details added to the database.");
    }
}
