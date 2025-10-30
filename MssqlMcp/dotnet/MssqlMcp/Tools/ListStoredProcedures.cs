// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    private const string ListStoredProceduresQuery = @"
        SELECT s.name AS SchemaName, p.name AS ProcedureName
        FROM sys.procedures p
        INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
        ORDER BY s.name, p.name
    ";

    [McpServerTool(
        Title = "List Stored Procedures",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Lists all stored procedures in the SQL Database.")]
    public async Task<DbOperationResult> ListStoredProcedures()
    {
        var conn = await _connectionFactory.GetOpenConnectionAsync();
        try
        {
            using (conn)
            {
                using var cmd = new SqlCommand(ListStoredProceduresQuery, conn);
                var procedures = new List<string>();
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    procedures.Add($"{reader.GetString(0)}.{reader.GetString(1)}");
                }
                return new DbOperationResult(success: true, data: procedures);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ListStoredProcedures failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}
