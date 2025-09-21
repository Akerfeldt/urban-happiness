using System.Data;
using System.Text;
using Azure;
using Azure.Storage.Blobs;
using Bogus;
using Dapper;
using MessagePack;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

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

    [Function("CountDb")]
    public IActionResult CountDbFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        return CountDb();
    }

    [Function("CountBlob")]
    public IActionResult CountBlobFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        return CountBlob();
    }

    [Function("SearchBlob")]
    public IActionResult SearchBlobFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        return SearchBlob(req);
    }

    [Function("SearchDb")]
    public IActionResult SearchDbFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        return SearchDb(req);
    }

    [Function("SearchDbLowCpu")]
    public IActionResult SearchDbLowCpuFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        return SearchDbLowCpu(req);
    }

    [Function("SeedBlob")]
    public async Task<IActionResult> SeedBlobFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        var blobClient = GetBlobClient();
        var blobUsers = GetBlobUsers(blobClient);

        var userFaker = new Faker<BlobUser>()
                .RuleFor(u => u.Id, f => f.Random.Int(min: 0))
                .RuleFor(u => u.DateOfBirth, f => f.Date.Past(50, DateTime.UtcNow.AddYears(-18)).ToString("yyyy-MM-dd"))
                .RuleFor(u => u.Email, (f, u) => f.Internet.Email(u.Name))
                .RuleFor(u => u.Location, f => f.Address.Country())
                .RuleFor(u => u.Name, f => f.Name.FullName())
                .RuleFor(u => u.Reputation, f => f.Random.Int(min: 0));
        var newUsers = userFaker.Generate(1000);
        blobUsers.AddRange(newUsers);

        var newBlobBytes = MessagePackSerializer.Serialize(blobUsers);
        blobClient.Upload(new MemoryStream(newBlobBytes), overwrite: true);

        return new OkObjectResult("");
    }

    private static List<BlobUser> GetBlobUsers(BlobClient blobClient)
    {
        try
        {
            var blobBytes = blobClient.DownloadContent().Value.Content.ToArray();
            var blobUsers = MessagePackSerializer.Deserialize<List<BlobUser>>(blobBytes);
            return blobUsers;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return [];
        }
    }

    [Function("SeedDb")]
    public async Task<IActionResult> SeedDbFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        var userFaker = new Faker<DbUser>()
                .RuleFor(u => u.DateOfBirth, f => f.Date.Past(50, DateTime.UtcNow.AddYears(-18)))
                .RuleFor(u => u.Email, (f, u) => f.Internet.Email(u.Name))
                .RuleFor(u => u.Location, f => f.Address.Country())
                .RuleFor(u => u.Name, f => f.Name.FullName())
                .RuleFor(u => u.Reputation, f => f.Random.Int(min: 0));

        DataTable usersDataTable = new DataTable("users");
        usersDataTable.Columns.Add("DateOfBirth", typeof(DateTime));
        usersDataTable.Columns.Add("Email", typeof(string));
        usersDataTable.Columns.Add("Location", typeof(string));
        usersDataTable.Columns.Add("Name", typeof(string));
        usersDataTable.Columns.Add("Reputation", typeof(int));

        var newUsers = userFaker.Generate(1000);
        foreach (var user in newUsers)
        {
            usersDataTable.Rows.Add(user.DateOfBirth, user.Email, user.Location, user.Name, user.Reputation);
        }

        using (SqlConnection connection = new(GetDbConnectionString()))
        {
            connection.Open();
            try
            {
                using SqlBulkCopy bulkCopy = new SqlBulkCopy(connection);
                bulkCopy.DestinationTableName = "users";
                bulkCopy.ColumnMappings.Add("DateOfBirth", "DateOfBirth");
                bulkCopy.ColumnMappings.Add("Email", "Email");
                bulkCopy.ColumnMappings.Add("Location", "Location");
                bulkCopy.ColumnMappings.Add("Name", "Name");
                bulkCopy.ColumnMappings.Add("Reputation", "Reputation");
                bulkCopy.WriteToServer(usersDataTable);
            }
            finally
            {
                connection.Close();
            }
        }

        return new OkObjectResult("");
    }

    private IActionResult CountDb()
    {
        var conn = new SqlConnection(GetDbConnectionString());
        var rowCount = conn.ExecuteScalar<int>("SELECT COUNT(*) FROM [dbo].[users]");

        return new OkObjectResult(rowCount);
    }

    private IActionResult CountBlob()
    {
        var blobClient = GetBlobClient();
        var blobUsers = GetBlobUsers(blobClient);
        return new OkObjectResult(blobUsers.Count);
    }

    private string? GetDbConnectionString()
    {
        return _configuration.GetConnectionString("searchdb");
    }

    private BlobClient GetBlobClient()
    {
        var connectionString = GetBlobConnectionString();
        var containerClient = new BlobContainerClient(connectionString, "users");
        containerClient.CreateIfNotExists();
        return containerClient.GetBlobClient("allusers");
    }

    private IActionResult SearchDb(HttpRequest req)
    {
        var options = GetSearchOptions(req);
        if (options.IsEmpty)
        {
            return new OkObjectResult(new List<UserDto>());
        }

        var conn = new SqlConnection(GetDbConnectionString());

        var sb = new StringBuilder();
        sb.Append("SELECT * FROM [dbo].[users] WHERE ");

        var parameters = new Dictionary<string, object>();
        if (!string.IsNullOrEmpty(options.Name))
        {
            sb.Append("[Name] LIKE @Name");
            parameters.Add("Name", $"%{EscapeSearchTerm(options.Name)}%");
        }

        if (!string.IsNullOrEmpty(options.Location))
        {
            sb.Append("[Location] LIKE @Location");
            parameters.Add("Location", $"%{EscapeSearchTerm(options.Location)}%");
        }
        var results = conn.Query<DbUser>(sb.ToString(), parameters);

        return new OkObjectResult(results);
    }

    private IActionResult SearchDbLowCpu(HttpRequest req)
    {
        var options = GetSearchOptions(req);
        if (options.IsEmpty)
        {
            return new OkObjectResult(new List<UserDto>());
        }

        var conn = new SqlConnection(GetDbConnectionString());
        var users = conn.Query<DbUser>("SELECT * FROM [dbo].[users]") as IList<DbUser>;

        var results = new List<UserDto>();
        foreach (var user in users!)
        {
            if (user == null)
                continue;

            if (results.Count >= 100)
            {
                break;
            }

            if (!string.IsNullOrEmpty(options.Name)
                && user.Name?.Contains(options.Name) != true)
            {
                continue;
            }

            if (!string.IsNullOrEmpty(options.Location)
                && user.Location?.Contains(options.Location) != true)
            {
                continue;
            }

            results.Add(new UserDto(user));
        }

        return new OkObjectResult(results);
    }

    private string EscapeSearchTerm(string s) => s.Replace("[", "[[").Replace("%", "[%]");

    private IActionResult SearchBlob(HttpRequest req)
    {
        var results = new List<UserDto>();
        var options = GetSearchOptions(req);
        if (options.IsEmpty)
        {
            return new OkObjectResult(results);
        }

        var blobClient = GetBlobClient();
        var blobUsers = GetBlobUsers(blobClient);
        foreach (var entity in blobUsers)
        {
            if (entity == null)
                continue;

            if (results.Count >= 100)
            {
                break;
            }

            if (!string.IsNullOrEmpty(options.Name)
                && entity.Name?.Contains(options.Name) != true)
            {
                continue;
            }

            if (!string.IsNullOrEmpty(options.Location)
                && entity.Location?.Contains(options.Location) != true)
            {
                continue;
            }

            results.Add(new UserDto(entity));
        }

        return new OkObjectResult(results);
    }

    private string? GetBlobConnectionString()
    {
        return _configuration.GetValue<string>("Aspire:Azure:Storage:Blobs:blobs:ConnectionString")
            ?? _configuration.GetConnectionString("blobs");
    }

    private SearchOptions GetSearchOptions(HttpRequest req)
    {
        var name = req.Query["name"];
        var location = req.Query["location"];
        return new SearchOptions(name, location);
    }

    record SearchOptions(string? Name, string? Location)
    {
        public bool IsEmpty => string.IsNullOrEmpty(Name) && string.IsNullOrEmpty(Location);
    }

    [MessagePackObject]
    public class BlobUser
    {
        [Key(0)]
        public int Id { get; set; }

        [Key(1)]
        public string? DateOfBirth { get; set; }

        [Key(2)]
        public string? Email { get; set; }

        [Key(3)]
        public string? Name { get; set; }

        [Key(4)]
        public string? Location { get; set; }

        [Key(5)]
        public int Reputation { get; set; }
    }

    public class DbUser
    {
        public int Id { get; set; }
        public DateTime? DateOfBirth { get; set; }
        public string? Email { get; set; }
        public string? Name { get; set; }
        public string? Location { get; set; }
        public int Reputation { get; set; }
    }

    public class UserDto
    {
        public int Id { get; set; }
        public DateOnly? DateOfBirth { get; set; }
        public string? Email { get; set; }
        public string? Name { get; set; }
        public string? Location { get; set; }
        public int Reputation { get; set; }

        public UserDto(DbUser source)
        {
            Id = source.Id;
            DateOfBirth = source.DateOfBirth == null ? null : DateOnly.FromDateTime(source.DateOfBirth.Value);
            Email = source.Email;
            Name = source.Name;
            Location = source.Location;
            Reputation = source.Reputation;
        }

        public UserDto(BlobUser source)
        {
            Id = source.Id;
            DateOfBirth = source.DateOfBirth == null ? null : DateOnly.ParseExact(source.DateOfBirth, "yyyy-MM-dd");
            Email = source.Email;
            Name = source.Name;
            Location = source.Location;
            Reputation = source.Reputation;
        }
    }
}