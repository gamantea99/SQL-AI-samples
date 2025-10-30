// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

using System.ComponentModel;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Server;
using System.Text;

namespace Mssql.McpServer;

public partial class Tools
{
    [McpServerTool(
        Title = "Batch Process Stored Procedures",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Processes stored procedures in batches and accumulates results.")]
    public async Task<DbOperationResult> BatchProcessStoredProcedures(
        [Description("Number of stored procedures to process in each batch")] int batchSize = 10,
        [Description("Optional schema filter")] string? schemaFilter = null,
        [Description("Optional name pattern to filter procedures (uses SQL LIKE pattern)")] string? namePattern = null,
        [Description("Whether to include procedure definitions in results")] bool includeDefinitions = false)
    {
        if (batchSize <= 0)
        {
            return new DbOperationResult(success: false, error: "Batch size must be greater than zero.");
        }

        var conn = await _connectionFactory.GetOpenConnectionAsync();
        try
        {
            using (conn)
            {
                // Query to get all stored procedures matching the filters
                var sb = new StringBuilder();
                sb.Append(@"
                    SELECT s.name AS SchemaName, p.name AS ProcedureName
                    FROM sys.procedures p
                    INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
                    WHERE 1=1");
                
                if (!string.IsNullOrWhiteSpace(schemaFilter))
                {
                    sb.Append(" AND s.name = @SchemaFilter");
                }
                
                if (!string.IsNullOrWhiteSpace(namePattern))
                {
                    sb.Append(" AND p.name LIKE @NamePattern");
                }
                
                sb.Append(" ORDER BY s.name, p.name");

                using var cmd = new SqlCommand(sb.ToString(), conn);
                
                if (!string.IsNullOrWhiteSpace(schemaFilter))
                {
                    cmd.Parameters.AddWithValue("@SchemaFilter", schemaFilter);
                }
                
                if (!string.IsNullOrWhiteSpace(namePattern))
                {
                    cmd.Parameters.AddWithValue("@NamePattern", namePattern);
                }

                // Get all procedures that match the criteria
                var procedures = new List<string>();
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        procedures.Add($"{reader.GetString(0)}.{reader.GetString(1)}");
                    }
                }

                // Process in batches
                var totalProcedures = procedures.Count;
                var batches = (int)Math.Ceiling((double)totalProcedures / batchSize);
                var batchResults = new List<Dictionary<string, object>>();
                
                for (int i = 0; i < batches; i++)
                {
                    var start = i * batchSize;
                    var count = Math.Min(batchSize, totalProcedures - start);
                    var batch = procedures.GetRange(start, count);
                    
                    var batchResult = new Dictionary<string, object>
                    {
                        ["batchNumber"] = i + 1,
                        ["procedures"] = await ProcessBatch(batch, conn, includeDefinitions)
                    };
                    
                    batchResults.Add(batchResult);
                }

                return new DbOperationResult(success: true, 
                    data: new Dictionary<string, object>
                    {
                        ["totalProcedures"] = totalProcedures,
                        ["batchCount"] = batches,
                        ["batchSize"] = batchSize,
                        ["results"] = batchResults
                    });
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "BatchProcessStoredProcedures failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }

    private async Task<List<Dictionary<string, object>>> ProcessBatch(List<string> procedures, SqlConnection conn, bool includeDefinitions)
    {
        var results = new List<Dictionary<string, object>>();
        
        foreach (var proc in procedures)
        {
            var parts = proc.Split('.');
            if (parts.Length != 2)
            {
                continue;
            }

            var schema = parts[0];
            var name = parts[1];
            
            var procInfo = new Dictionary<string, object>
            {
                ["schema"] = schema,
                ["name"] = name,
                ["fullName"] = proc
            };
            
            if (includeDefinitions)
            {
                // Get the definition
                var sql = @"
                    SELECT definition
                    FROM sys.sql_modules sm
                    INNER JOIN sys.objects o ON sm.object_id = o.object_id
                    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                    WHERE o.type = 'P' AND s.name = @Schema AND o.name = @Name";
                
                using var cmd = new SqlCommand(sql, conn);
                cmd.Parameters.AddWithValue("@Schema", schema);
                cmd.Parameters.AddWithValue("@Name", name);
                
                var definition = await cmd.ExecuteScalarAsync() as string;
                if (definition != null)
                {
                    procInfo["definition"] = definition;
                }
            }
            
            results.Add(procInfo);
        }
        
        return results;
    }
}