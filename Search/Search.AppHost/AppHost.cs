var builder = DistributedApplication.CreateBuilder(args);

var storage = builder.AddAzureStorage("storage")
                     .RunAsEmulator(
                     azurite =>
                     {
                         azurite.WithDataVolume();
                     });

var blobs = storage.AddBlobs("blobs");

var sql = builder.AddSqlServer("sql")
                 .WithDataVolume();

var databaseName = "searchdb";
var creationScript = $$"""
    IF DB_ID('{{databaseName}}') IS NULL
        CREATE DATABASE [{{databaseName}}];
    GO

    USE [{{databaseName}}];
    GO

    CREATE TABLE users (
        Id INT PRIMARY KEY IDENTITY(1,1),
        DateOfBirth DATE,
        Email VARCHAR(255),
        Location VARCHAR(255),
        Name VARCHAR(255),
        Reputation INT
    );
    GO

    """;

var db = sql.AddDatabase(databaseName)
            .WithCreationScript(creationScript);

builder.AddAzureFunctionsProject<Projects.SearchFn>("searchfn")
       .WithHostStorage(storage)
       .WithReference(blobs)
       .WithReference(db);

builder.Build().Run();
