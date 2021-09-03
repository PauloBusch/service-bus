using EmitterServiceBus.Models;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace EmitterServiceBus.HostedServices
{
    public class ProductQueueConsumer : IHostedService
    {
        private static IQueueClient _queueClient;

        public ProductQueueConsumer(IConfiguration configuration)
        {
            _queueClient = new QueueClient(configuration.GetConnectionString("AzureServiceBus"), "product");
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("################ Starting Consumer - Queue ###################");
            ProcessMessageHandler();
            await Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("################ Stopping Consumer - Queue ###################");
            await _queueClient.CloseAsync();
        }

        private void ProcessMessageHandler()
        {
            var messageHalderOptions = new MessageHandlerOptions(ExceptionReceivedHandlerAsync) {
                MaxConcurrentCalls = 1,
                AutoComplete = false
            };

            _queueClient.RegisterMessageHandler(ProcessMessageHandlerAsync, messageHalderOptions);
        }

        private async Task ProcessMessageHandlerAsync(Message message, CancellationToken token)
        {
            Console.WriteLine("#### Processing Message - Queue ###");
            Console.WriteLine(DateTime.Now);
            Console.WriteLine($"Received message: SequenceNumber: {message.SystemProperties.SequenceNumber} Body: {Encoding.UTF8.GetString(message.Body)}");
            var product = JsonSerializer.Deserialize<Product>(Encoding.UTF8.GetString(message.Body));

            await _queueClient.CompleteAsync(message.SystemProperties.LockToken);
        }

        private async Task ExceptionReceivedHandlerAsync(ExceptionReceivedEventArgs exception)
        {
            Console.WriteLine($"Message handler enconuntred an exception {exception.Exception}");
            var context = exception.ExceptionReceivedContext;
            Console.WriteLine("Exception context for troublesHooting:");
            Console.WriteLine($"- Endpoint: {context.Endpoint}");
            Console.WriteLine($"- Entity Path: {context.EntityPath}");
            Console.WriteLine($"- Action: {context.Action}");
            await Task.CompletedTask;
        }
    }
}
