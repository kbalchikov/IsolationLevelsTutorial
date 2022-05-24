using Npgsql;

namespace IsolationLevels.Tests;

public class ConnectionFactory
{
    private const string ConnectionString = "Host=localhost;Username=postgres;Password=password;Database=IsolationLevelsDB";

    public static NpgsqlConnection GetConnection()
    {
        var connection = new NpgsqlConnection(ConnectionString);
        connection.Open();
        return connection;
    }
}
