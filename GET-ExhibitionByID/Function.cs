using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Lambda.APIGatewayEvents;
using Amazon.Lambda.Core;
using Newtonsoft.Json;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace GET_ExhibitionByID;

public class Function
{

    public async Task<APIGatewayHttpApiV2ProxyResponse> FunctionHandler(APIGatewayHttpApiV2ProxyRequest request, ILambdaContext context)
    {
        var log = context.Logger;
        log.LogInformation($"Request received: {JsonConvert.SerializeObject(request, Formatting.Indented)}");

        try
        {
            if (!request.QueryStringParameters.TryGetValue("exhibitionID", out string exhibitionID) || string.IsNullOrEmpty(exhibitionID))
            {
                log.LogError("userID is missing or empty in the query parameters");
                return new APIGatewayHttpApiV2ProxyResponse
                {
                    StatusCode = 400,
                    Body = JsonConvert.SerializeObject(new { message = "userID is missing or empty in the query parameters." })
                };
            }

            log.LogInformation($"Querying tables for exhibitionID: {exhibitionID}");

            List<Dictionary<string, AttributeValue>> results;
            try
            {
                results = await QueryTables(exhibitionID);
                log.LogInformation($"Query completed. Number of results: {results.Count}");
            }
            catch (Exception ex)
            {
                log.LogError($"Error in QueryTables: {ex.Message}");
                return new APIGatewayHttpApiV2ProxyResponse
                {
                    StatusCode = 500,
                    Body = JsonConvert.SerializeObject(new { message = "An error occurred while querying the database.", error = ex.Message })
                };
            }

            if (results != null)
            {
                var transformedResults = results.Select(item => new
                {
                    ExhibitionID = item.GetValueOrDefault("ExhibitionID")?.N,
                    ExhibitionName = item.GetValueOrDefault("ExhibitionName")?.S,
                    ExhibitionLength = item.GetValueOrDefault("ExhibitionLength")?.N,
                    ExhibitionImage = item.GetValueOrDefault("ExhibitionImage")?.S,
                    ExhibitionPublic = item.GetValueOrDefault("ExhibitionPublic")?.N ?? item.GetValueOrDefault("ExhibitionPublic")?.S,
                    ExhibitContent = item.GetValueOrDefault("ExhibitContent")?.L?.Select(content => new
                    {
                        CreationDate = content.M?.GetValueOrDefault("CreationDate")?.ToString(),
                        ItemCreditline = content.M?.GetValueOrDefault("ItemCreditline")?.ToString(),
                        ItemDepartment = content.M?.GetValueOrDefault("ItemDepartment")?.ToString(),
                        ItemID = content.M?.GetValueOrDefault("ItemID")?.ToString(),
                        ItemClassification = content.M?.GetValueOrDefault("ItemClassification")?.ToString(),
                        ItemTechnique = content.M?.GetValueOrDefault("ItemTechnique")?.ToString(),
                        ItemTitle = content.M?.GetValueOrDefault("ItemTitle")?.ToString(),
                        ItemURL = content.M?.GetValueOrDefault("ItemImageURL")?.ToString(),
                        ItemObjectLink = content.M?.GetValueOrDefault("ItemObjectLink")?.ToString(),
                        ItemCentury = content.M?.GetValueOrDefault("ItemCentury")?.ToString(),

                    }).ToList()
                }).ToList();


                log.LogInformation($"Query completed. Result: {transformedResults}");
                return new APIGatewayHttpApiV2ProxyResponse
                {
                    StatusCode = 200,
                    Body = JsonConvert.SerializeObject(new { exhibitions = transformedResults })
                };
            }

            log.LogInformation($"Query completed. No Matching Exhibitions");
            return new APIGatewayHttpApiV2ProxyResponse
            {
                StatusCode = 200,
                Body = JsonConvert.SerializeObject(new { exhibitions = "" })
            };

        }
        catch (Exception ex)
        {
            log.LogError($"Unexpected error in FunctionHandler: {ex.Message}");
            return new APIGatewayHttpApiV2ProxyResponse
            {
                StatusCode = 500,
                Body = JsonConvert.SerializeObject(new { message = "An unexpected error occurred.", error = ex.Message })
            };
        }
    }

    private async Task<List<Dictionary<string, AttributeValue>>> QueryTables(string exhibitionID)
    {
        var dynamoDbClient = new AmazonDynamoDBClient();

        try
        {
            var publicTableTask = QueryTable(dynamoDbClient, "PublicExhibitions", exhibitionID);
            var privateTableTask = QueryTable(dynamoDbClient, "PrivateExhibitions", exhibitionID);

            await Task.WhenAll(publicTableTask, privateTableTask);

            var combinedResults = new List<Dictionary<string, AttributeValue>>();
            combinedResults.AddRange(publicTableTask.Result);
            combinedResults.AddRange(privateTableTask.Result);

            return combinedResults;
        }
        catch (Exception ex)
        {
            throw new Exception($"Error in QueryTables: {ex.Message}", ex);
        }
    }

    private async Task<List<Dictionary<string, AttributeValue>>> QueryTable(IAmazonDynamoDB dynamoDbClient, string tableName, string exhibitionID)
    {
        try
        {
            var scanRequest = new ScanRequest
            {
                TableName = tableName,
                FilterExpression = "ExhibitionID = :exhibitionId",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue>
            {
                {":exhibitionId", new AttributeValue { N = exhibitionID }}
            }
            };

            var scanResponse = await dynamoDbClient.ScanAsync(scanRequest);
            return scanResponse.Items;
        }
        catch (Exception ex)
        {
            throw new Exception($"Error querying table {tableName}: {ex.Message}", ex);
        }
    }
}
