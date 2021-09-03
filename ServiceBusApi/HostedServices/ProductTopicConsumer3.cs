using EmitterServiceBus.Models;
using Microsoft.Azure.ServiceBus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using ServiceBusApi.ViewModels;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace EmitterServiceBus.HostedServices
{
    public class ProductTopicConsumer3 : IHostedService
    {
        private static ISubscriptionClient _subscriptionClient;

        public ProductTopicConsumer3(IConfiguration configuration)
        {
            _subscriptionClient = new SubscriptionClient(configuration.GetConnectionString("AzureServiceBus"), "store", "store-sub3");
        }

        public async Task StartAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("################ Starting Consumer - Topic Product Sub 3 ###################");
            ProcessMessageHandler();
            await Task.CompletedTask;
        }

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            Console.WriteLine("################ Stopping Consumer - Topic Product Sub 3 ###################");
            await _subscriptionClient.CloseAsync();
        }

        private void ProcessMessageHandler()
        {
            var messageHalderOptions = new MessageHandlerOptions(ExceptionReceivedHandlerAsync) {
                MaxConcurrentCalls = 3,
                AutoComplete = false
            };

            _subscriptionClient.RegisterMessageHandler(ProcessMessageHandlerAsync, messageHalderOptions);
        }

        private async Task ProcessMessageHandlerAsync(Message message, CancellationToken token)
        {
            Console.WriteLine("#### Processing Message - Topic Product Sub 3 ###");
            Console.WriteLine(DateTime.Now);
            Console.WriteLine($"Received message: SequenceNumber: {message.SystemProperties.SequenceNumber} Body: {Encoding.UTF8.GetString(message.Body)}");
            var product = JsonSerializer.Deserialize<ProductViewModel>(Encoding.UTF8.GetString(message.Body));

            await _subscriptionClient.CompleteAsync(message.SystemProperties.LockToken);
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
