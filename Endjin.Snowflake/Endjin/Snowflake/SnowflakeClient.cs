// <copyright file="SnowflakeClient.cs" company="Endjin">
// Copyright (c) Endjin. All rights reserved.
// </copyright>

namespace Endjin.Snowflake
{
    using System.Collections.Generic;
    using System.Data;
    using System.Dynamic;
    using global::Snowflake.Data.Client;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// A client for submitting queries to Snowflake.
    /// </summary>
    public class SnowflakeClient
    {
        private readonly string connectionString;

        /// <summary>
        /// Initializes a new instance of the <see cref="SnowflakeClient"/> class.
        /// </summary>
        /// <param name="connectionString">The Snowflake connection string to use.</param>
        public SnowflakeClient(string connectionString)
        {
            this.connectionString = connectionString;
        }

        /// <summary>
        /// Executes a sequence of Snowflake statements that are not expected to return a result set.
        /// </summary>
        /// <param name="statements">The query statements to execute.</param>
        /// <returns>The number of rows affected. When more than one statement is supplied the method will return the number of rows affected for the last statement only.</returns>
        public int ExecuteNonQuery(params string[] statements)
        {
            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = this.connectionString;
                conn.Open();

                IDbCommand cmd = conn.CreateCommand();

                int affectedRows = 0;
                foreach (string command in statements)
                {
                    cmd.CommandText = command;
                    affectedRows = cmd.ExecuteNonQuery();
                }

                return affectedRows;
            }
        }

        /// <summary>
        /// Executes a sequence of Snowflake statements that are expected to return a result set.
        /// </summary>
        /// <param name="statements">The query statements to execute.</param>
        /// <returns>JObject containing the result set from the execution of the last statement in set.</returns>
        public JObject ExecuteReader(params string[] statements)
        {
            var rows = new JArray();
            var row = new JObject();
            var output = new JObject();

            using (IDbConnection conn = new SnowflakeDbConnection())
            {
                conn.ConnectionString = this.connectionString;
                conn.Open();

                using (IDbCommand cmd = conn.CreateCommand())
                {
                    // Run every query except the last one using ExecuteNonQuery
                    for (int i = 0; i < statements.Length - 1; i++)
                    {
                        cmd.CommandText = statements[i].Trim();
                        cmd.ExecuteNonQuery();
                    }

                    // Finally run the last query using ExecuteReader() so we can collect the output
                    cmd.CommandText = statements[statements.Length - 1].Trim();
                    IDataReader reader = cmd.ExecuteReader();

                    // The result should be a table with m rows and n columns, format the column/value pairs in JSON
                    while (reader.Read())
                    {
                        row = new JObject();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            string columnName = reader.GetName(i);
                            string value = reader[i].ToString();
                            row.Add(columnName, value);
                        }

                        rows.Add(row);
                    }

                    output.Add("rows", rows);
                }

                conn.Close();
            }

            return output;
        }
    }
}
