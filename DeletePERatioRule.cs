using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Collections.Generic;

public static class DeleteRuleFunction
{
    private static HttpClient httpClient = new HttpClient();

    [FunctionName("DeletePERatioRule")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "delete", Route = "DeletePERatioRule/{ruleId}")] HttpRequest req,
        ILogger log,
        int ruleId)
    {
        log.LogInformation($"C# HTTP trigger function initiated to delete rule with ID: {ruleId}");

        // URL of the SQLInterface function replace with production URL as necessary.
        string sqlInterfaceUrl = "http://localhost:7144/api/SQLInterface";

        // SQL query to delete a rule.
        string sqlQuery = "DELETE FROM dbo.PERatioRules WHERE RuleID = @RuleID";

        // Prepare the request data with the SQL query and its parameters.
        var requestData = new
        {
            sql = sqlQuery,
            parameters = new Dictionary<string, object>
            {
                { "@RuleID", ruleId }
            }
        };

        // Serialise the request data into JSON format for the HTTP request.
        var requestContent = new StringContent(JsonConvert.SerializeObject(requestData), Encoding.UTF8, "application/json");

        try
        {
            // Execute the HTTP POST request to the SQL interface URL.
            var response = await httpClient.PostAsync(sqlInterfaceUrl, requestContent);

            // Check response status and log error.
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"Failed to delete P/E ratio rule with ID: {ruleId}. Status code: {response.StatusCode}");
                return new StatusCodeResult((int)response.StatusCode);
            }

            // Confirm deletion.
            log.LogInformation($"Rule with ID: {ruleId} deleted successfully.");
            return new OkResult();
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
}
