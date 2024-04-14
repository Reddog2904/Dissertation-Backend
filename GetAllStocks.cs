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

namespace TradingBotAzureFunctions
{
    public static class GetAllStocks
    {
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("GetAllStocks")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request to get all stocks.");

            // Setup the SQL query and parameters.
            var sqlData = new
            {
                sql = "SELECT CompanyID, Symbol, Name, Sector, Industry FROM [dbo].[Companies]",
                parameters = new { }
            };

            var sqlDataJson = JsonConvert.SerializeObject(sqlData);
            var content = new StringContent(sqlDataJson, Encoding.UTF8, "application/json");

            var sqlInterfaceUrl = "http://localhost:7144/api/SQLInterface";

            // Send a POST request to the SQLInterface function and await the response.
            var response = await httpClient.PostAsync(sqlInterfaceUrl, content);

            // Check if the response from the SQLInterface function was successful.
            if (response.IsSuccessStatusCode)
            {
                string resultContent = await response.Content.ReadAsStringAsync();
                return new OkObjectResult(resultContent); // Directly return the JSON result from the SQLInterface.
            }
            else
            {
                // Read the error content, log the error, and return an error status code.
                string errorContent = await response.Content.ReadAsStringAsync();
                log.LogError($"Failed to retrieve stocks. Status: {response.StatusCode}, Content: {errorContent}");
                return new StatusCodeResult((int)response.StatusCode);
            }
        }
    }
}
