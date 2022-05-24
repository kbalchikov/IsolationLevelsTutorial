using FluentMigrator.Runner;
using IsolationLevels.Migrations;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Sharprompt;

namespace IsolationLevels.Runner;

public static class Program
{
    private const string ConnectionString = "Host=localhost;Username=postgres;Password=password;Database=IsolationLevelsDB";

    static void Main(string[] args)
    {
        var builder = new NpgsqlConnectionStringBuilder(ConnectionString);
        if (string.IsNullOrEmpty(builder.Database))
        {
            Console.WriteLine($"Database not specified in connection string");
            return;
        }

        Console.WriteLine($"Trying to connect to database {builder.Database} on host {builder.Host} under username {builder.Username}");

        string dbName = builder.Database;
        builder.Database = "postgres";

        if (!IsDatabaseExists(builder, dbName))
        {
            bool create = Prompt.Confirm($"Database {dbName} does not exist. Create?");
            if (!create)
                return;

            CreateDatabase(builder, dbName);
        }

        var serviceProvider = CreateServices();
        using var scope = serviceProvider.CreateScope();
        var runner = scope.ServiceProvider.GetService<IMigrationRunner>();
        var versionLoader = scope.ServiceProvider.GetService<IVersionLoader>();

        Console.WriteLine();

        Console.WriteLine($"Current version: {versionLoader.VersionInfo.Latest()}");
        runner.ListMigrations();

        while (true)
        {
            Console.WriteLine("==========================");

            string selected = Prompt.Select("Choose action:", new[]
            {
                "Migrate to latest",
                "Migrate to version",
                "Rollback latest",
                "Rollback to version",
                "List migrations"
            });

            switch (selected)
            {
                case "Migrate to latest":
                    runner.MigrateUp();
                    break;

                case "Migrate to version":
                    long version = Prompt.Input<long>("Input version number");

                    if (!runner.HasMigrationsToApplyUp(version))
                        Console.WriteLine($"No migrations to apply up to version {version}");
                    else
                        runner.MigrateUp(version);

                    break;

                case "Rollback latest":
                    if (!runner.HasMigrationsToApplyRollback())
                        Console.WriteLine("No migration to rollback");
                    else
                        runner.Rollback(1);
                    break;

                case "Rollback to version":
                    long rollbackVersion = Prompt.Input<long>("Input version number");
                    runner.RollbackToVersion(rollbackVersion);
                    break;

                case "List migrations":
                    runner.ListMigrations();
                    break;
            }
        }
    }

    /// <summary>
    /// Configure the dependency injection services
    /// </summary>
    private static IServiceProvider CreateServices()
    {
        var services = new ServiceCollection()
            .AddFluentMigratorCore()
            .ConfigureRunner(rb => rb
                .AddPostgres11_0()
                .WithGlobalConnectionString(ConnectionString)
                .ScanIn(typeof(Initial).Assembly).For.Migrations().For.EmbeddedResources())
            .AddLogging(lb => lb.AddFluentMigratorConsole());

        return services.BuildServiceProvider(false);
    }

    private static void CreateDatabase(NpgsqlConnectionStringBuilder builder, string dbName)
    {
        try
        {
            var connection = new NpgsqlConnection(builder.ConnectionString);
            var command = new NpgsqlCommand($"CREATE DATABASE \"{dbName}\";", connection);

            connection.Open();
            command.ExecuteNonQuery();
            connection.Close();
        }
        catch (PostgresException exc) when (exc.SqlState == "42P04")
        {
            Console.WriteLine("Database already exists");
        }
    }

    private static bool IsDatabaseExists(NpgsqlConnectionStringBuilder builder, string dbName)
    {
        try
        {
            var connection = new NpgsqlConnection(builder.ConnectionString);
            var command = new NpgsqlCommand($"SELECT 1 FROM pg_database WHERE datname='{dbName}';", connection);

            connection.Open();
            int? result = (int?)command.ExecuteScalar();
            connection.Close();

            return result == 1;
        }
        catch (PostgresException)
        {
            return false;
        }
    }
}
