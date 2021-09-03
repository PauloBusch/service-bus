using EmitterServiceBus.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace EmitterServiceBus.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class ProductsController : ControllerBase
    {
        private readonly string _azureServiceBusConnectionString;

        public ProductsController(IConfiguration configuration)
        {
            _azureServiceBusConnectionString = configuration.GetConnectionString("AzureServiceBus");
        }

        [HttpPost("queue")]
        public async Task<object> PostQueueAsync([FromBody] Product product)
        {
            await SendMessageQueue(product);
            return new { Success = true, Data = product };
        }

        [HttpPost("topic")]
        public async Task<object> PostTopicAsync([FromBody] Product product)
        {
            await SendMessageTopic(product);
            return new { Success = true, Data = product };
        }

        private async Task SendMessageQueue(Product product)
        {
            var queueName = "product";
            var client = new QueueClient(_azureServiceBusConnectionString, queueName, ReceiveMode.PeekLock);
            var message = new Message(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(product)));

            await client.SendAsync(message);
            await client.CloseAsync();
        }

        private async Task SendMessageTopic(Product product)
        {
            var topicName = "store";
            var client = new TopicClient(_azureServiceBusConnectionString, topicName);
            var message = new Message(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(product)));

            await client.SendAsync(message);
            await client.CloseAsync();
        }
    }
}
