using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;
using Amazon.DynamoDBv2.Model;
using Amazon.EventBridge;
using Amazon.EventBridge.Model;
using Amazon.Lambda.Core;
using Amazon.Lambda.DynamoDBEvents;
using Newtonsoft.Json;
using JsonSerializer = Amazon.Lambda.Serialization.Json.JsonSerializer;

[assembly: LambdaSerializer(typeof(JsonSerializer))]

namespace StreamProcessor
{
    public static class Extensions
    {
        public static dynamic ToObject(this AttributeValue value)
        {
            dynamic obj = new ExpandoObject();

            if (!string.IsNullOrEmpty(value.N))
            {
                obj = value.N.Contains(".") 
                    ? Convert.ToDecimal(value.N) 
                    : Convert.ToInt64(value.N);
            }
            else if (!string.IsNullOrEmpty(value.S))
            {
                obj = value.S;
            }
            else if (value.IsMSet)
                foreach (var item in value.M)
                    ((IDictionary<string, dynamic>) obj)[item.Key] = item.Value.ToObject();

            else if (value.IsLSet)
            {
                obj = new List<dynamic>();
                foreach (var item in value.L)
                    obj.Add(item.ToObject());
            }
            else if (value.IsBOOLSet)
            {
                obj = value.BOOL;
            }
            else if (value.NULL)
            {
                obj = null;
            }
            else if (value.B != null)
            {
                obj = Convert.ToBase64String(value.B.ToArray());
            }
            else if (value.BS.Count > 0)
            {
                obj = new List<string>();
                foreach (var item in value.BS)
                    obj.Add(Convert.ToBase64String(item.ToArray()));
            }
            else if (value.SS.Count > 0)
            {
                obj = new List<string>();
                foreach (var item in value.SS)
                    obj.Add(item);
            }
            else if (value.NS.Count > 0)
            {
                obj = new List<decimal>();
                foreach (var item in value.NS)
                    obj.Add(Convert.ToDecimal(item));
            }
            else throw new Exception("Unable to convert property.");

            return obj;
        }
    }

    public class Function
    {
        private const int MaxEventsPerPut = 10;

        private readonly IAmazonEventBridge eventBridge = new AmazonEventBridgeClient();

        private readonly string eventBusName = Environment.GetEnvironmentVariable("EventBusName");
        private readonly string source = Environment.GetEnvironmentVariable("EventSourceName");


        public async Task FunctionHandler(DynamoDBEvent dynamoEvent, ILambdaContext context)
        {
            context.Logger.LogLine($"Beginning to process {dynamoEvent.Records.Count} records...");

            var groups =
                from i in dynamoEvent.Records.Select((value, index) =>
                    new {Value = value, Index = index})
                group i.Value by i.Index / MaxEventsPerPut
                into g
                select g;

            var tasks = new List<Task<PutEventsResponse>>();

            foreach (var group in groups)
            {
                var entries = new List<PutEventsRequestEntry>();
                foreach (var record in group)
                {
                    var detailType = $"{record.EventSourceArn.Split('/')[1]}-{record.EventName.Value}";

                    dynamic item = new ExpandoObject();

                    Dictionary<string, AttributeValue> image = null;

                    switch (record.EventName.Value)
                    {
                        case "INSERT":
                        case "MODIFY":
                            image = record.Dynamodb.NewImage;
                            break;
                        case "REMOVE":
                            image = record.Dynamodb.OldImage;
                            break;
                    }
                    //context.Logger.Log(JsonConvert.SerializeObject(image));
                    foreach (var property in image)
                    {
                        //context.Logger.LogLine(property.Key);
                        ((IDictionary<string, dynamic>)item)[property.Key] = property.Value.ToObject();
                    }

                    entries.Add(new PutEventsRequestEntry
                    {
                        EventBusName = eventBusName,
                        Detail = JsonConvert.SerializeObject(item),
                        DetailType = detailType,
                        Source = source
                    });
                }

                tasks.Add(eventBridge.PutEventsAsync(new PutEventsRequest
                {
                    Entries = entries
                }));
            }

            await Task.WhenAll(tasks);

            context.Logger.LogLine("Stream processing complete.");
        }
    }
}