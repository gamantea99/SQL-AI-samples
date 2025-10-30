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
        Title = "Find Tables With Column",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Finds all tables in a schema that contain (or do not contain) a specified column.")]
    public async Task<DbOperationResult> FindTablesWithColumn(
        [Description("Schema name to search (e.g. AMB_DEV_271)")] string schema,
        [Description("Column name to search for (case-insensitive)")] string columnName)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(columnName))
        {
            return new DbOperationResult(success: false, error: "Schema and column name must be provided.");
        }

        var conn = await _connectionFactory.GetOpenConnectionAsync();
        try
        {
            using (conn)
            {
                // Get all tables in the schema
                var tablesQuery = @"
                    SELECT t.TABLE_SCHEMA, t.TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES t
                    WHERE t.TABLE_TYPE = 'BASE TABLE'
                    ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
                ";
                using var tablesCmd = new SqlCommand(tablesQuery, conn);
                tablesCmd.Parameters.AddWithValue("@Schema", schema);

                var tables = new List<(string Schema, string Name)>();
                using (var reader = await tablesCmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        tables.Add((reader.GetString(0), reader.GetString(1)));
                    }
                }

                // For each table, check if the column exists
                var matches = new List<string>();
                var nonMatches = new List<string>();

                foreach (var (tblSchema, tblName) in tables)
                {
                    var columnQuery = @"
                        SELECT COUNT(*) 
                        FROM INFORMATION_SCHEMA.COLUMNS 
                        WHERE TABLE_SCHEMA = @Schema AND TABLE_NAME = @Table AND LOWER(COLUMN_NAME) = LOWER(@Column)
                    ";
                    using var colCmd = new SqlCommand(columnQuery, conn);
                    colCmd.Parameters.AddWithValue("@Schema", tblSchema);
                    colCmd.Parameters.AddWithValue("@Table", tblName);
                    colCmd.Parameters.AddWithValue("@Column", columnName);

                    var scalarResult = await colCmd.ExecuteScalarAsync();
                    var count = (scalarResult != null && scalarResult != DBNull.Value) ? Convert.ToInt32(scalarResult) : 0;
                    var fullName = $"{tblSchema}.{tblName}";
                    if (count > 0)
                    {
                        matches.Add(fullName);
                    }
                    else
                    {
                        nonMatches.Add(fullName);
                    }
                }

                var result = new Dictionary<string, object>
                {
                    ["matchingTables"] = matches,
                    ["nonMatchingTables"] = nonMatches
                };

                return new DbOperationResult(success: true, data: result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "FindTablesWithColumn failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}