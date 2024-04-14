using System;
using System.Data.SqlClient;
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
    public static class SQLInterface
    {
        [FunctionName("SQLInterface")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {

            // Read and deserialise HTTP request body.
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string sql = data?.sql;

            // Deserialise SQL parameters from JSON.
            var parameters = JsonConvert.DeserializeObject<Dictionary<string, object>>(data?.parameters?.ToString());

            // Connection string for SQL database.
            string connectionString = "SQL-Server-Connection-String";

            try
            {
                // Open SQL connection.
                using (var conn = new SqlConnection(connectionString))
                {
                    conn.Open();
                    // Create SQL command with parameters.
                    using (var cmd = new SqlCommand(sql, conn))
                    {
                        // Add parameters to the SQL command.
                        if (parameters != null)
                        {
                            foreach (var param in parameters)
                            {
                                cmd.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
                            }
                        }

                        // Execute SQL command and process results.
                        using (var reader = await cmd.ExecuteReaderAsync())
                        {
                            var results = new List<Dictionary<string, object>>();

                            // Read each row and populate results list.
                            while (reader.Read())
                            {
                                var row = new Dictionary<string, object>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    row.Add(reader.GetName(i), reader.GetValue(i));
                                }
                                results.Add(row);
                            }

                            // Return results as JSON.
                            return new OkObjectResult(JsonConvert.SerializeObject(results)); // Serialize and return the list of rows
                        }
                    }
                }

            }
            catch (SqlException sqlEx)
            {
                log.LogError($"SQL Exception: {sqlEx.Message}");
                return new StatusCodeResult(500);
            }
            catch (Exception ex)
            {
                log.LogError($"Exception: {ex.Message}");
                return new StatusCodeResult(500);
            }
        }
    }
}
