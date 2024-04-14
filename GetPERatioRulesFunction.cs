using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Generic;

public static class GetPERatioRulesFunction
{
    private static HttpClient httpClient = new HttpClient();

    [FunctionName("GetPERatioRules")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
        ILogger log)
    {
        log.LogInformation("C# HTTP trigger function to get P/E ratio rules.");

        // URL for the SQLInterface function should be updated with production URL in deployment.
        string sqlInterfaceUrl = "http://localhost:7144/api/SQLInterface";

        // SQL query to fetch P/E ratio rules from the database.
        string sqlQuery = "SELECT RuleID, BuyLevel, SellLevel FROM dbo.PERatioRules";

        try
        {
            // Setup the request data with the SQL query.
            var requestData = new
            {
                sql = sqlQuery,
                parameters = new Dictionary<string, object>()
            };

            // Serialise the request data into JSON and prepare the HTTP content.
            var requestContent = new StringContent(JsonConvert.SerializeObject(requestData), Encoding.UTF8, "application/json");

            // Send a POST request to the SQL interface and await the response.
            var response = await httpClient.PostAsync(sqlInterfaceUrl, requestContent);

            // Check if the HTTP response is successful.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Failed to get P/E ratio rules. Status code: {response.StatusCode}");
                return new StatusCodeResult((int)response.StatusCode);
            }

            // Read the JSON response content and deserialize into a list of P/E ratio rules.
            var responseData = await response.Content.ReadAsStringAsync();
            var rules = JsonConvert.DeserializeObject<List<PERatioRuleData>>(responseData);

            // Return the rules as a successful object result.
            return new OkObjectResult(rules);
        }
        catch (HttpRequestException httpEx)
        {
            log.LogError($"HTTP Request Exception: {httpEx.Message}");
            return new StatusCodeResult(StatusCodes.Status503ServiceUnavailable);
        }
        catch (JsonException jsonEx)
        {
            log.LogError($"JSON Exception: {jsonEx.Message}");
            return new StatusCodeResult(StatusCodes.Status500InternalServerError);
        }
    }

    // Represents the buying and selling thresholds for P/E ratio rules.
    public class PERatioRuleData
    {
        public int RuleID { get; set; }
        public decimal BuyLevel { get; set; }
        public decimal SellLevel { get; set; }
    }
}
