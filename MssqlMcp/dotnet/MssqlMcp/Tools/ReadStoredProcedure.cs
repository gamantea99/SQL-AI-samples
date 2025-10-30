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
        Title = "Read Stored Procedure",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Reads the T-SQL definition of a stored procedure from the database")]
    public async Task<DbOperationResult> ReadStoredProcedure(
        [Description("Name of the stored procedure")] string procedureName)
    {
        if (string.IsNullOrWhiteSpace(procedureName))
        {
            return new DbOperationResult(success: false, error: "Procedure name must not be empty.");
        }

        var conn = await _connectionFactory.GetOpenConnectionAsync();
        try
        {
            using (conn)
            {
                var sql = @"
                    SELECT sm.definition
                    FROM sys.sql_modules sm
                    INNER JOIN sys.objects o ON sm.object_id = o.object_id
                    WHERE o.type = 'P' AND o.name = @ProcedureName
                ";
                using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@ProcedureName", procedureName);
                var definition = await cmd.ExecuteScalarAsync() as string;
                if (definition == null)
                {
                    return new DbOperationResult(success: false, error: $"Stored procedure '{procedureName}' not found.");
                }
                return new DbOperationResult(success: true, data: definition);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "ReadStoredProcedure failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}
