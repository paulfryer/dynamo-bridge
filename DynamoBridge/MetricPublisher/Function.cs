using System.Threading.Tasks;
using Amazon.Lambda.Core;
using Amazon.Lambda.Serialization.Json;

[assembly: LambdaSerializer(typeof(JsonSerializer))]

namespace MetricPublisher
{
    public class Function
    {
        public async Task FunctionHandler(dynamic @event, ILambdaContext context)
        {



        }
    }
}