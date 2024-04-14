using System;
using System.IO;
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
    public static class DeleteStockFromDatabase
    {
        private static readonly HttpClient httpClient = new HttpClient();

        [FunctionName("DeleteStockFromDatabase")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request to delete a stock symbol.");

            // Read and deserialise the request body.
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string ticker = data?.ticker;

            // Validate the ticker symbol from the request.
            if (string.IsNullOrEmpty(ticker))
            {
                return new BadRequestObjectResult("Please pass a stock symbol in the request body.");
            }

            // Prepare the SQL command and parameters for deletion.
            var sqlData = new
            {
                sql = "DELETE FROM dbo.Companies WHERE Symbol = @ticker;",
                parameters = new { ticker = ticker }
            };

            // Serialise the SQL command and parameters to JSON.
            var sqlDataJson = JsonConvert.SerializeObject(sqlData);
            var content = new StringContent(sqlDataJson, Encoding.UTF8, "application/json");

            // Send the serialised SQL command to the SQLInterface function.
            var sqlInterfaceUrl = "http://localhost:7144/api/SQLInterface";
            var response = await httpClient.PostAsync(sqlInterfaceUrl, content);
            if (response.IsSuccessStatusCode)
            {
                // Return success response if SQL command was executed successfully.
                return new OkObjectResult($"Stock symbol {ticker} deleted successfully from Companies table.");
            }
            else
            {
                // Read error content and log the error if SQL command execution failed.
                string errorContent = await response.Content.ReadAsStringAsync();
                log.LogError($"Failed to execute SQL command. Status: {response.StatusCode}, Content: {errorContent}");
                return new StatusCodeResult((int)response.StatusCode);
            }
        }
    }
}
