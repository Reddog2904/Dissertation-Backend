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
using System.IO;

namespace TradingBotAzureFunctions
{
    public static class AddPERatioRuleFunction
    {
        [FunctionName("AddPERatioRule")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function to add P/E ratio rule.");

            // Read the request body.
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            // Deserialise JSON into PERatioRuleData object.
            PERatioRuleData data = JsonConvert.DeserializeObject<PERatioRuleData>(requestBody);

            // Check if data was successfully parsed.
            if (data == null)
            {
                // Return error if data is null.
                return new BadRequestObjectResult("Please pass the rule data in the request body");
            }

            // Prepare SQL command string with parameter placeholders.
            var sql = "INSERT INTO dbo.PERatioRules (BuyLevel, SellLevel) VALUES (@BuyLevel, @SellLevel)";
            var parameters = new Dictionary<string, object>
            {
                { "@BuyLevel", data.BuyLevel },
                { "@SellLevel", data.SellLevel }
            };

            // Bundle the SQL command and parameters into one object for the HTTP request.
            var sqlInterfaceRequest = new
            {
                sql = sql,
                parameters = parameters
            };

            // Serialize the SQL command and parameters into JSON for the HTTP body.
            var content = new StringContent(JsonConvert.SerializeObject(sqlInterfaceRequest), Encoding.UTF8, "application/json");

            // Set the URL for the SQL interface function; change this for production.
            string sqlInterfaceUrl = "http://localhost:7144/api/SQLInterface";

            // Create an HttpClient instance and send a POST request.
            using (var httpClient = new HttpClient())
            {
                var response = await httpClient.PostAsync(sqlInterfaceUrl, content);

                if (!response.IsSuccessStatusCode)
                {
                    log.LogError($"Failed to add P/E ratio rule. Status code: {response.StatusCode}");
                    return new StatusCodeResult((int)response.StatusCode);
                }
                return new OkResult();
            }
        }

        // Defines a class to represent P/E ratio rule data.
        public class PERatioRuleData
        {
            public decimal BuyLevel { get; set; }
            public decimal SellLevel { get; set; }
        }
    }
}
