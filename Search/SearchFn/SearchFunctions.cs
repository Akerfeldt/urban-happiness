using Azure.Storage.Blobs;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;

namespace SearchFn;

public class SearchFunctions
{
    private readonly ILogger<SearchFunctions> _logger;
    private readonly IConfiguration _configuration;

    public SearchFunctions(ILogger<SearchFunctions> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    [Function("ByApi")]
    public IActionResult Run([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        return SearchInApi(req);
    }

    private IActionResult SearchInApi(HttpRequest req)
    {
        var options = GetSearchOptions(req);

        var connectionString = _configuration.GetValue<string>("Aspire:Azure:Storage:Blobs:blobs:ConnectionString");
        var blobServiceClient = new BlobServiceClient(connectionString);
        var containers = blobServiceClient.GetBlobContainers();
        var container = containers.FirstOrDefault(x => x.Name == "users");

        return new OkObjectResult("");
    }

    private SearchOptions GetSearchOptions(HttpRequest req)
    {
        var name = req.Query["name"];
        var location = req.Query["location"];
        var reputation = req.Query["reputation"] == StringValues.Empty ? (int?)null : int.Parse(req.Query["reputation"]);
        return new SearchOptions(name, location, reputation);
    }

    record SearchOptions(string? Name, string? Location, int? Reputation);
}