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
        Title = "Describe Stored Procedure",
        ReadOnly = true,
        Idempotent = true,
        Destructive = false),
        Description("Returns stored procedure metadata and parameters.")]
    public async Task<DbOperationResult> DescribeStoredProcedure(
        [Description("Name of stored procedure (optionally schema-qualified)")] string name)
    {
        string? schema = null;
        if (name.Contains('.'))
        {
            var parts = name.Split('.');
            if (parts.Length > 1)
            {
                name = parts[1];
                schema = parts[0];
            }
        }
        const string ProcInfoQuery = @"SELECT p.object_id AS id, p.name, s.name AS [schema], p.create_date, p.modify_date, ep.value AS description
            FROM sys.procedures p
            INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
            LEFT JOIN sys.extended_properties ep ON ep.major_id = p.object_id AND ep.minor_id = 0 AND ep.name = 'MS_Description'
            WHERE p.name = @ProcName and (s.name = @ProcSchema or @ProcSchema IS NULL) ";

        const string ParamsQuery = @"SELECT name, type_name(user_type_id) AS type, max_length, precision, scale, is_output, parameter_id
            FROM sys.parameters
            WHERE object_id = (SELECT object_id FROM sys.procedures p INNER JOIN sys.schemas s ON p.schema_id = s.schema_id WHERE p.name = @ProcName and (s.name = @ProcSchema or @ProcSchema IS NULL ))
            ORDER BY parameter_id";

        var conn = await _connectionFactory.GetOpenConnectionAsync();
        try
        {
            using (conn)
            {
                var result = new Dictionary<string, object>();
                // Procedure info
                using (var cmd = new SqlCommand(ProcInfoQuery, conn))
                {
                    var _ = cmd.Parameters.AddWithValue("@ProcName", name);
                    _ = cmd.Parameters.AddWithValue("@ProcSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    if (await reader.ReadAsync())
                    {
                        result["procedure"] = new
                        {
                            id = reader["id"],
                            name = reader["name"],
                            schema = reader["schema"],
                            create_date = reader["create_date"],
                            modify_date = reader["modify_date"],
                            description = reader["description"] is DBNull ? null : reader["description"]
                        };
                    }
                    else
                    {
                        return new DbOperationResult(success: false, error: $"Stored procedure '{name}' not found.");
                    }
                }
                // Parameters
                using (var cmd = new SqlCommand(ParamsQuery, conn))
                {
                    var _ = cmd.Parameters.AddWithValue("@ProcName", name);
                    _ = cmd.Parameters.AddWithValue("@ProcSchema", schema == null ? DBNull.Value : schema);
                    using var reader = await cmd.ExecuteReaderAsync();
                    var parameters = new List<object>();
                    while (await reader.ReadAsync())
                    {
                        parameters.Add(new
                        {
                            name = reader["name"],
                            type = reader["type"],
                            max_length = reader["max_length"],
                            precision = reader["precision"],
                            scale = reader["scale"],
                            is_output = reader["is_output"],
                            parameter_id = reader["parameter_id"]
                        });
                    }
                    result["parameters"] = parameters;
                }
                return new DbOperationResult(success: true, data: result);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "DescribeStoredProcedure failed: {Message}", ex.Message);
            return new DbOperationResult(success: false, error: ex.Message);
        }
    }
}
