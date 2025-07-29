using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

namespace CosmosDbScript
{
    public class Program
    {
        // Replace with your Cosmos DB endpoint and key
        private static readonly string EndpointUri = Environment.GetEnvironmentVariable("COSMOS_DB_ENDPOINT_URI");
        private static readonly string PrimaryKey = Environment.GetEnvironmentVariable("COSMOS_DB_PRIMARY_KEY");

        private static CosmosClient cosmosClient;
        private static Database database;
        private static Container container;

        private static string databaseId = "YourDatabaseName";
        private static string containerId = "YourContainerName";
        private static string partitionKeyPath = "/YourPartitionKeyPath"; // Example: "/category"

        public static async Task Main(string[] args)
        {
            if (string.IsNullOrEmpty(EndpointUri) || string.IsNullOrEmpty(PrimaryKey))
            {
                Console.WriteLine("COSMOS_DB_ENDPOINT_URI and COSMOS_DB_PRIMARY_KEY environment variables must be set.");
                Environment.Exit(1);
            }

            try
            {
                // Initialize the Cosmos DB client
                cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);

                // Create database if it doesn't exist
                database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
                Console.WriteLine($"Database '{database.Id}' ready.");

                // Create container if it doesn't exist
                container = await database.CreateContainerIfNotExistsAsync(containerId, partitionKeyPath);
                Console.WriteLine($"Container '{container.Id}' ready.");

                // Example: Write a new item
                await CreateItemAsync();

                // Example: Read an item
                await ReadItemAsync("item1");

                // Example: Query items
                await QueryItemsAsync("Electronics");
            }
            catch (CosmosException ex)
            {
                Console.WriteLine($"Cosmos DB operation failed: {ex.StatusCode} - {ex.Message}");
                // [Business Logic Complexity] Implement more specific error handling based on status codes.
                Environment.Exit(1);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An unexpected error occurred: {ex.Message}");
                Environment.Exit(1);
            }
            finally
            {
                // Dispose the client when done
                cosmosClient?.Dispose();
            }
        }

        // Represents a simple document structure
        public class MyDocument
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Category { get; set; }
            public int Quantity { get; set; }
            public string PartitionKey { get; set; } // This should map to your container's partition key path
        }

        private static async Task CreateItemAsync()
        {
            MyDocument newItem = new MyDocument
            {
                Id = "item1",
                Name = "Laptop",
                Category = "Electronics",
                Quantity = 50,
                PartitionKey = "Electronics" // Must match the partition key of the container
            };

            ItemResponse<MyDocument> createResponse = await container.CreateItemAsync(newItem, new PartitionKey(newItem.PartitionKey));
            Console.WriteLine($"Created item: {createResponse.Resource.Id} (RU: {createResponse.RequestCharge})");
        }

        private static async Task ReadItemAsync(string id)
        {
            // [Business Logic Complexity] For point reads, you need both Id and Partition Key.
            try
            {
                ItemResponse<MyDocument> readResponse = await container.ReadItemAsync<MyDocument>(id, new PartitionKey("Electronics"));
                MyDocument item = readResponse.Resource;
                Console.WriteLine($"Read item: {item.Id}, Name: {item.Name} (RU: {readResponse.RequestCharge})");
            }
            catch (CosmosException ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                Console.WriteLine($"Item with ID '{id}' not found.");
            }
        }

        private static async Task QueryItemsAsync(string category)
        {
            QueryDefinition queryDefinition = new QueryDefinition("SELECT * FROM c WHERE c.Category = @category")
                .WithParameter("@category", category);

            using FeedIterator<MyDocument> feedIterator = container.GetItemQueryIterator<MyDocument>(queryDefinition);

            List<MyDocument> results = new List<MyDocument>();
            double totalRequestCharge = 0;

            while (feedIterator.HasMoreResults)
            {
                FeedResponse<MyDocument> response = await feedIterator.ReadNextAsync();
                results.AddRange(response.ToList());
                totalRequestCharge += response.RequestCharge;
            }

            Console.WriteLine($"Query for category '{category}' returned {results.Count} items (Total RU: {totalRequestCharge}):");
            foreach (var item in results)
            {
                Console.WriteLine($"- {item.Id}: {item.Name}");
            }
        }
    }
}
