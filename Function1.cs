using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace functions_csharp_eventhub_sql
{
    public static class Function1
    {
        private static SqlConnection sqlConnection = new SqlConnection(System.Environment.GetEnvironmentVariable("SqlConnectionString"));
        private static object _lockObj;
        [FunctionName("Function1")]
        public static async Task Run([EventHubTrigger("my-event-hub", Connection = "EventHubsConnectionString")] EventData[] events, ILogger log)
        {
            var exceptions = new List<Exception>();
            // Make sure there is an open SqlConnection
            // Because this is Event Hubs batch I know only one execution will happen on one instance at a time
            if(sqlConnection.State != System.Data.ConnectionState.Open)
            {
                await sqlConnection.OpenAsync();
            }

            foreach (EventData eventData in events)
            {
                try
                {
                    string messageBody = Encoding.UTF8.GetString(eventData.Body.Array, eventData.Body.Offset, eventData.Body.Count);

                    // Replace these two lines with your processing logic.
                    log.LogInformation($"C# Event Hub trigger function processed a message: {messageBody}");
                    using (SqlCommand command = new SqlCommand("SOME_SQL_COMMAND", sqlConnection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                Console.WriteLine("{0} {1}", reader.GetString(0), reader.GetString(1));
                            }
                        }
                    }
                    await Task.Yield();
                }
                catch (Exception e)
                {
                    // We need to keep processing the rest of the batch - capture this exception and continue.
                    // Also, consider capturing details of the message that failed processing so it can be processed again later.
                    exceptions.Add(e);
                }
            }

            // Once processing of the batch is complete, if any messages in the batch failed processing throw an exception so that there is a record of the failure.

            if (exceptions.Count > 1)
                throw new AggregateException(exceptions);

            if (exceptions.Count == 1)
                throw exceptions.Single();
        }
    }
}
