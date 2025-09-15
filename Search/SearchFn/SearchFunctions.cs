using System.Data;
using System.Text;
using Azure;
using Azure.Data.Tables;
using Bogus;
using Dapper;
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

    [Function("SearchTable")]
    public IActionResult SearchTableFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        return SearchTable(req);
    }

    [Function("SearchDb")]
    public IActionResult SearchDbFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        return SearchDb(req);
    }

    [Function("SeedTable")]
    public async Task<IActionResult> SeedTableFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        var tableClient = GetTableClient();
        await tableClient.CreateIfNotExistsAsync();

        var userFaker = new Faker<TableUser>()
                .RuleFor(u => u.Id, f => f.Random.Int(min: 0))
                .RuleFor(u => u.DateOfBirth, f => f.Date.Past(50, DateTime.UtcNow.AddYears(-18)).ToString("yyyy-MM-dd"))
                .RuleFor(u => u.Email, (f, u) => f.Internet.Email(u.Name))
                .RuleFor(u => u.Location, f => f.Address.Country())
                .RuleFor(u => u.Name, f => f.Name.FullName())
                .RuleFor(u => u.Reputation, f => f.Random.Int(min: 0));

        for (int i = 0; i < 10; i++)
        {
            var newUsers = userFaker.Generate(100);
            var actions = new List<TableTransactionAction>();
            foreach (var user in newUsers)
            {
                var action = new TableTransactionAction(TableTransactionActionType.Add, user);
                actions.Add(action);
            }
            tableClient.SubmitTransaction(actions);
        }

        return new OkObjectResult("");
    }

    [Function("SeedDb")]
    public async Task<IActionResult> SeedDbFunction([HttpTrigger(AuthorizationLevel.Function, "get")] HttpRequest req)
    {
        var tableClient = GetTableClient();
        await tableClient.CreateIfNotExistsAsync();

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

        var connectionString = _configuration.GetConnectionString("searchdb");
        var conn = new SqlConnection(connectionString);
        using (SqlConnection connection = new(connectionString))
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

    private TableClient GetTableClient()
    {
        var connectionString = GetTableConnectionString();
        var tableClient = new TableClient(connectionString, "users");
        return tableClient;
    }

    private IActionResult SearchDb(HttpRequest req)
    {
        var options = GetSearchOptions(req);
        if (options.IsEmpty)
        {
            return new OkObjectResult(new List<UserDto>());
        }

        var connectionString = _configuration.GetConnectionString("searchdb");
        var conn = new SqlConnection(connectionString);

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

    private string EscapeSearchTerm(string s) => s.Replace("[", "[[").Replace("%", "[%]");

    private IActionResult SearchTable(HttpRequest req)
    {
        var results = new List<UserDto>();
        var options = GetSearchOptions(req);
        if (options.IsEmpty)
        {
            return new OkObjectResult(results);
        }

        var tableClient = GetTableClient();
        Pageable<TableUser> queryResults = tableClient.Query<TableUser>(maxPerPage: 1000);
        foreach (var entity in queryResults)
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

    private string? GetTableConnectionString()
    {
        return _configuration.GetValue<string>("Aspire:Azure:Data:Tables:tables:ConnectionString")
            ?? _configuration.GetConnectionString("tables");
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

    public class TableUser : ITableEntity
    {
        public int Id { get; set; }
        public string? DateOfBirth { get; set; }
        public string? Email { get; set; }
        public string? Name { get; set; }
        public string? Location { get; set; }
        public int Reputation { get; set; }
        public string PartitionKey { get => (Id / 100).ToString(); set => ((Action)(() => { }))(); }
        public string RowKey { get => Id.ToString(); set => ((Action)(() => { }))(); }
        public DateTimeOffset? Timestamp { get; set; } = DateTimeOffset.UtcNow;
        public ETag ETag { get; set; }
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

        public UserDto(TableUser source)
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