using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.IO;

namespace TradingBotAzureFunctions
{
    public static class PerformBacktest
    {
        private static readonly HttpClient client = new HttpClient();

        [FunctionName("PerformBacktest")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request for backtesting.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync(); // Read the entire HTTP request body.
            dynamic data = JsonConvert.DeserializeObject(requestBody); // Deserialise the request body into a dynamic object.

            // Validate and convert 'ruleId' from the request body into an integer.
            if (!int.TryParse((string)data?.ruleId, out int ruleId))
            {
                log.LogError($"Invalid ruleId: {data?.ruleId}");
                return new BadRequestObjectResult("Invalid ruleId provided.");
            }

            // Retrieve trading rule from the database using 'ruleId'.
            var rule = await FetchTradingRuleAsync(ruleId, log);
            if (rule == null || !rule.ContainsKey("BuyLevel") || !rule.ContainsKey("SellLevel"))
            {
                return new BadRequestObjectResult("Rule not found or is missing BuyLevel/SellLevel.");
            }

            // Fetch list of companies from the database.
            var companies = await FetchCompaniesAsync(log);
            if (companies == null)
            {
                return new BadRequestObjectResult("Failed to retrieve companies list.");
            }

            // Extract 'startDate' and 'endDate' from the request body.
            string startDate = data?.startDate;
            string endDate = data?.endDate;

            // Execute backtest across multiple companies using the fetched rule and dates.
            var processingResult = await ExecuteBacktestForMultipleCompaniesAsync(companies, rule, startDate, endDate, log);

            // Return HTTP 200 OK with the results of the backtesting.
            return new OkObjectResult(processingResult);
        }

        // Asynchronously deserialises the HTTP request body into a dynamic object.
        private static async Task<dynamic> DeserializeRequestBodyAsync(HttpRequest req)
        {
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            return JsonConvert.DeserializeObject(requestBody);
        }

        // Asynchronously retrieves and deserialises a trading rule.
        private static async Task<Dictionary<string, decimal>> FetchTradingRuleAsync(int ruleId, ILogger log)
        {
            string sqlQuery = @"SELECT BuyLevel, SellLevel FROM dbo.PERatioRules WHERE RuleID = @RuleID";
            var parameters = new Dictionary<string, object> { { "@RuleID", ruleId } };

            string jsonResponse = await CallSQLInterfaceAsync(sqlQuery, parameters, log);
            if (string.IsNullOrEmpty(jsonResponse))
            {
                log.LogError($"Failed to fetch trading rule for RuleID: {ruleId}");
                return null; // Or handle this case as needed.
            }

            try
            {
                // Deserialise the JSON response to get the rule.
                var ruleList = JsonConvert.DeserializeObject<List<Dictionary<string, decimal>>>(jsonResponse);
                return ruleList?.FirstOrDefault(); // Returns the first rule or null if the list is empty.
            }
            catch (JsonException ex)
            {
                log.LogError($"Error deserializing rule data: {ex.Message}");
                return null; // Or handle this case as needed.
            }
        }

        // Asynchronously fetches and deserialises a list of companies from the database.
        private static async Task<List<Dictionary<string, string>>> FetchCompaniesAsync(ILogger log)
        {
            var companiesResponse = await CallSQLInterfaceAsync(
                "SELECT CompanyID, Symbol FROM dbo.Companies",
                new Dictionary<string, object>(),
                log
            );
            return JsonConvert.DeserializeObject<List<Dictionary<string, string>>>(companiesResponse);
        }

        // Coordinates the backtesting process across multiple companies.
        private static async Task<object> ExecuteBacktestForMultipleCompaniesAsync(
    List<Dictionary<string, string>> companies,
    Dictionary<string, decimal> rule,
    string startDate,
    string endDate,
    ILogger log)
        {
            decimal initialTotalBalance = 10000000; // Start with 10,000,000 currency units.
            decimal allocationPerCompany = initialTotalBalance / companies.Count; // Divide equally among companies.
            var allTrades = new List<Trade>();
            int tradeNumber = 1;
            decimal finalTotalBalance = 0; // This will be used to accumulate the final balance from each company.

            foreach (var company in companies)
            {
                var result = await ExecuteBacktestForSingleCompanyAsync(
                    company,
                    rule,
                    startDate,
                    endDate,
                    tradeNumber,
                    allocationPerCompany, // This is the balance for individual company backtest.
                    log);

                allTrades.AddRange(result.Trades);
                finalTotalBalance += result.FinalBalance; // Update the final total balance directly.
                tradeNumber = result.TradeNumber;
            }

            // Calculate the percentage difference between the final total balance and the original balance.
            decimal profitLossPercentage = CalculateProfitLossPercentage(finalTotalBalance, initialTotalBalance);

            // Log the final total balance and the percentage difference.
            log.LogInformation($"Final total balance: {finalTotalBalance:C2}. Percentage difference: {profitLossPercentage:N2}%");

            // Return the result.
            return new
            {
                Trades = allTrades,
                FinalTotalBalance = finalTotalBalance,
                ProfitLossPercentage = profitLossPercentage
            };
        }

        // Executes backtesting for a single company using specified trading rules and date range, returning the trading results.
        static async Task<(List<Trade> Trades, decimal FinalBalance, int TradeNumber)> ExecuteBacktestForSingleCompanyAsync(
    Dictionary<string, string> company,
    Dictionary<string, decimal> rule,
    string startDate,
    string endDate,
    int tradeNumber,
    decimal allocationPerCompany,
    ILogger log)
        {
            string companyId = company["CompanyID"];
            string companySymbol = company["Symbol"];
            decimal buyThreshold = rule["BuyLevel"];
            decimal sellThreshold = rule["SellLevel"];
            decimal balance = allocationPerCompany; // Starting balance for the company.
            decimal sharesOwned = 0;
            bool ownsShares = false; // Indicates if shares are currently owned.
            var trades = new List<Trade>();

            var peRatios = await FetchPeRatiosForCompanyAsync(companyId, startDate, endDate, log);
            if (peRatios == null)
            {
                log.LogWarning($"No PE ratios found for {companySymbol}. Skipping backtest for this company.");
                return (new List<Trade>(), balance, tradeNumber);
            }

            foreach (var peRatioRecord in peRatios)
            {
                decimal peRatio = Convert.ToDecimal(peRatioRecord["Value"]);
                DateTime dateOfRecord = Convert.ToDateTime(peRatioRecord["DateOfRecord"]);
                decimal pricePerShare = await FetchSharePriceAsync(companyId, dateOfRecord, log);

                // Buy logic.
                if (!ownsShares && peRatio < buyThreshold && balance >= pricePerShare)
                {
                    decimal sharesToBuy = Math.Floor(balance / pricePerShare);
                    balance -= Decimal.Round(sharesToBuy * pricePerShare, 4); // Update balance after buying.
                    sharesOwned += sharesToBuy;
                    ownsShares = true; // Now owns shares.
                    trades.Add(new Trade
                    {
                        Number = tradeNumber++,
                        Symbol = companySymbol,
                        Type = "Buy",
                        Date = dateOfRecord,
                        PERatio = peRatio,
                        PricePerShare = pricePerShare,
                        BankAccountBalance = balance,
                        Shares = sharesToBuy
                    });

                    log.LogInformation($"Bought {sharesToBuy} shares of {companySymbol} at {pricePerShare} each (P/E: {peRatio}) on {dateOfRecord:yyyy-MM-dd}. New balance: {balance:C4}.");
                }
                // Sell logic.
                else if (ownsShares && peRatio > sellThreshold)
                {
                    decimal sharesToSell = sharesOwned;
                    decimal revenue = Decimal.Round(sharesToSell * pricePerShare, 4);
                    balance += revenue; // Update balance after selling.
                    sharesOwned = 0; // Reset shares owned.
                    ownsShares = false; // No longer owns shares.
                    trades.Add(new Trade
                    {
                        Number = tradeNumber++,
                        Symbol = companySymbol,
                        Type = "Sell",
                        Date = dateOfRecord,
                        PERatio = peRatio,
                        PricePerShare = pricePerShare,
                        BankAccountBalance = balance,
                        Shares = sharesToSell
                    });

                    log.LogInformation($"Sold {sharesToSell} shares of {companySymbol} at {pricePerShare} each (P/E: {peRatio}) on {dateOfRecord:yyyy-MM-dd}. Revenue: {revenue:C4}. New balance: {balance:C4}.");
                }
            }

            // Finalise the remaining shares if any.
            if (ownsShares)
            {
                var lastPeRatio = peRatios.Last();
                decimal lastPricePerShare = await FetchSharePriceAsync(companyId, Convert.ToDateTime(lastPeRatio["DateOfRecord"]), log);
                decimal revenue = Decimal.Round(sharesOwned * lastPricePerShare, 4); // Calculate the revenue from the final sell-off.
                balance += revenue; // Update balance with the final sell-off revenue.
                log.LogInformation($"Sold {sharesOwned} shares of {companySymbol} at {lastPricePerShare} each on {Convert.ToDateTime(lastPeRatio["DateOfRecord"]):yyyy-MM-dd}. Revenue: {revenue:C4}. New balance: {balance:C4}.");

                trades.Add(new Trade
                {
                    Number = tradeNumber++,
                    Symbol = companySymbol,
                    Type = "Sell",
                    Date = Convert.ToDateTime(lastPeRatio["DateOfRecord"]),
                    PERatio = Convert.ToDecimal(lastPeRatio["Value"]),
                    PricePerShare = lastPricePerShare,
                    BankAccountBalance = balance,
                    Shares = sharesOwned
                });

                sharesOwned = 0; // Ensure shares are set to zero after final sell-off.
            }

            return (trades, balance, tradeNumber); // Return the results for this company.
        }




        // Utility method to calculate profit/loss percentage.
        private static decimal CalculateProfitLossPercentage(decimal finalBalance, decimal initialBalance)
        {
            return (finalBalance - initialBalance) / initialBalance * 100;
        }

        // Definition of the Trade class used in the lists.
        public class Trade
        {
            public int Number { get; set; }
            public string Symbol { get; set; }
            public string Type { get; set; } // "Buy" or "Sell".
            public DateTime Date { get; set; }
            public decimal PERatio { get; set; }
            public decimal PricePerShare { get; set; }
            public decimal BankAccountBalance { get; set; }
            public decimal Shares { get; set; }
        }

        // Asynchronously retrieves the share price for a specified company and date from the database.
        private static async Task<decimal> FetchSharePriceAsync(string companyId, DateTime dateOfRecord, ILogger log)
        {
            var sharePriceSql = @"
SELECT PricePerShare 
FROM dbo.Prices 
WHERE CompanyID = @CompanyID 
AND Date = @DateOfRecord";

            var parameters = new Dictionary<string, object>
    {
        { "@CompanyID", companyId },
        { "@DateOfRecord", dateOfRecord.ToString("yyyy-MM-dd") } // Ensuring the date format matches the database.
    };

            var sharePriceResponse = await CallSQLInterfaceAsync(sharePriceSql, parameters, log);
            var sharePriceRecord = JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(sharePriceResponse)?.FirstOrDefault();

            // If there's a valid record, return the PricePerShare; otherwise, return 0 as a fallback.
            return sharePriceRecord != null ? Convert.ToDecimal(sharePriceRecord["PricePerShare"]) : 0;
        }

        // Constructs and returns a new Trade object with specified details.
        private static Trade CreateTrade(int number, string symbol, string type, DateTime date, decimal peRatio, decimal pricePerShare, decimal balance)
        {
            return new Trade
            {
                Number = number,
                Symbol = symbol,
                Type = type, // "Buy" or "Sell".
                Date = date,
                PERatio = peRatio,
                PricePerShare = pricePerShare,
                BankAccountBalance = balance
            };
        }

        // Asynchronously retrieves a list of PE ratios for a specified company within a given date range.
        private static async Task<List<Dictionary<string, object>>> FetchPeRatiosForCompanyAsync(string companyId, string startDate, string endDate, ILogger log)
        {
            var peRatioSql = $@"
SELECT DateOfRecord, Value 
FROM dbo.PERatios 
WHERE CompanyID = @CompanyID 
AND DateOfRecord BETWEEN @StartDate AND @EndDate 
ORDER BY DateOfRecord";

            var parameters = new Dictionary<string, object>
    {
        { "@CompanyID", companyId },
        { "@StartDate", startDate },
        { "@EndDate", endDate }
    };

            var peRatiosResponse = await CallSQLInterfaceAsync(peRatioSql, parameters, log);
            return JsonConvert.DeserializeObject<List<Dictionary<string, object>>>(peRatiosResponse);
        }

        // Executes the SQL query through the SQLInterface function and returns the JSON response.
        private static async Task<string> CallSQLInterfaceAsync(string sql, Dictionary<string, object> parameters, ILogger log)
        {
            var payload = new
            {
                sql,
                parameters
            };

            var content = new StringContent(JsonConvert.SerializeObject(payload), Encoding.UTF8, "application/json");
            var response = await client.PostAsync("http://localhost:7144/api/SQLInterface", content);
            if (!response.IsSuccessStatusCode)
            {
                log.LogError($"SQLInterface call failed: {response.ReasonPhrase}");
                return null;
            }

            return await response.Content.ReadAsStringAsync();
        }

    }
}
