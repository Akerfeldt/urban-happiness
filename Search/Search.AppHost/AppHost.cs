var builder = DistributedApplication.CreateBuilder(args);

var storage = builder.AddAzureStorage("storage")
                     .RunAsEmulator(
                     azurite =>
                     {
                         azurite.WithDataVolume();
                     });

var blobs = storage.AddBlobs("blobs");

builder.AddAzureFunctionsProject<Projects.SearchFn>("searchfn")
       .WithHostStorage(storage)
       .WithReference(blobs);

builder.Build().Run();
