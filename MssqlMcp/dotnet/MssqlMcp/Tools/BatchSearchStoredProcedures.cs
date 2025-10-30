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
        Title = "Batch Search Stored Procedures",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Finds stored procedures by name pattern and reference, returning a summary of how the reference is used.")]
    public async Task<DbOperationResult> BatchSearchStoredProcedures(
        [Description("Pattern to match in stored procedure name (SQL LIKE, e.g. %Visit%)")] string namePattern,
        [Description("Reference text to search for in procedure definitions")] string reference,
        [Description("Batch size for processing")] int batchSize = 20)
    {
        if (string.IsNullOrWhiteSpace(namePattern) || string.IsNullOrWhiteSpace(reference))
        {
            return new DbOperationResult(success: false, error: "Both namePattern and reference are required.");
        }

        var conn = await _connectionFactory.GetOpenConnectionAsync();
        try
        {
            using (conn)
            {
                // 1. Get all procedures matching the name pattern
                var procQuery = @"
                    SELECT s.name AS SchemaName, p.name AS ProcedureName
                    FROM sys.procedures p
                    INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
                    WHERE p.name LIKE @NamePattern
                    ORDER BY s.name, p.name";
                using var cmd = new SqlCommand(procQuery, conn);
                cmd.Parameters.AddWithValue("@NamePattern", namePattern);

                var procedures = new List<(string Schema, string Name)>();
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        procedures.Add((reader.GetString(0), reader.GetString(1)));
                    }
                }

                var results = new List<Dictionary<string, object>>();
                for (int i = 0; i < procedures.Count; i += batchSize)
                {
                    var batch = procedures.Skip(i).Take(batchSize);
                    foreach (var (schema, name) in batch)
                    {
                        // 2. Get definition for each procedure
                        var defQuery = @"
                            SELECT sm.definition
                            FROM sys.sql_modules sm
                            INNER JOIN sys.objects o ON sm.object_id = o.object_id
                            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                            WHERE o.type = 'P' AND s.name = @Schema AND o.name = @Name";
                        using var defCmd = new SqlCommand(defQuery, conn);
                        defCmd.Parameters.AddWithValue("@Schema", schema);
                        defCmd.Parameters.AddWithValue("@Name", name);
                        var definition = await defCmd.ExecuteScalarAsync() as string;
                        if (definition != null && definition.Contains(reference, StringComparison.OrdinalIgnoreCase))
                        {
                            // 3. Extract a brief summary of how the reference is used (first line containing the reference)
                            var summary = ExtractReferenceSummary(definition, reference);
                            results.Add(new Dictionary<string, object>
                            {
                                ["schema"] = schema,
                                ["name"] = name,
                                ["fullName"] = $"{schema}.{name}",
                                ["referenceSummary"] = summary
                            });
                        }
                    }
                }

                return new DbOperationResult(success: true, data: results);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "BatchSearchStoredProcedures failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }

    private static string ExtractReferenceSummary(string definition, string reference)
    {
        // Find the first line containing the reference, trim and return up to 200 chars
        var lines = definition.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
        foreach (var line in lines)
        {
            if (line.IndexOf(reference, StringComparison.OrdinalIgnoreCase) >= 0)
            {
                var trimmed = line.Trim();
                return trimmed.Length > 200 ? trimmed.Substring(0, 200) + "..." : trimmed;
            }
        }
        // If not found, return empty
        return string.Empty;
    }
}