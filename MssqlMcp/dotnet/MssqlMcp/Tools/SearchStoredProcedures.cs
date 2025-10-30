// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;

namespace Mssql.McpServer;

public partial class Tools
{
    [McpServerTool(
        Title = "Search Stored Procedures",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Searches for stored procedures containing specific text in their definition.")]
    public async Task<DbOperationResult> SearchStoredProcedures(
        [Description("Text to search for in stored procedure definitions")] string searchText)
    {
        if (string.IsNullOrWhiteSpace(searchText))
        {
            return new DbOperationResult(success: false, error: "Search text must not be empty.");
        }

        var conn = await _connectionFactory.GetOpenConnectionAsync();
        try
        {
            using (conn)
            {
                var sql = @"
                    SELECT s.name AS SchemaName, o.name AS ProcedureName, sm.definition
                    FROM sys.sql_modules sm
                    INNER JOIN sys.objects o ON sm.object_id = o.object_id
                    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                    WHERE o.type = 'P' AND sm.definition LIKE '%' + @SearchText + '%'
                    ORDER BY s.name, o.name
                ";
                using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@SearchText", searchText);
                
                var results = new List<Dictionary<string, string>>();
                using var reader = await cmd.ExecuteReaderAsync();
                while (await reader.ReadAsync())
                {
                    results.Add(new Dictionary<string, string>
                    {
                        ["schema"] = reader.GetString(0),
                        ["name"] = reader.GetString(1),
                        ["fullName"] = $"{reader.GetString(0)}.{reader.GetString(1)}"
                    });
                }
                
                return new DbOperationResult(
                    success: true, 
                    data: new Dictionary<string, object> {
                        ["matchCount"] = results.Count,
                        ["procedures"] = results
                    });
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "SearchStoredProcedures failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}