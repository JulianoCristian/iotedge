// Copyright (c) Microsoft. All rights reserved.

namespace DirectMethodReceiver
{
    using System;
    using System.Globalization;
    using System.IO;
    using System.Net;
    using System.Runtime.Loader;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Microsoft.Extensions.Configuration;

    class Program
    {
        public static int Main() => MainAsync().Result;

        static async Task<int> MainAsync()
        {
            Console.WriteLine($"[{DateTime.UtcNow.ToString("MM/dd/yyyy hh:mm:ss.fff tt", CultureInfo.InvariantCulture)}] Main()");

            IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("config/appsettings.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            Console.WriteLine("[Configuration]");
            Console.WriteLine($"EdgeHubConnectionString={configuration.GetValue<string>("EdgeHubConnectionString")}");
            Console.WriteLine($"IOTEDGE_WORKLOADURI={configuration.GetValue<string>("IOTEDGE_WORKLOADURI")}");
            Console.WriteLine($"IOTEDGE_DEVICEID={configuration.GetValue<string>("IOTEDGE_DEVICEID")}");
            Console.WriteLine($"IOTEDGE_MODULEID={configuration.GetValue<string>("IOTEDGE_MODULEID")}");
            Console.WriteLine($"IOTEDGE_IOTHUBHOSTNAME={configuration.GetValue<string>("IOTEDGE_IOTHUBHOSTNAME")}");
            Console.WriteLine($"IOTEDGE_AUTHSCHEME={configuration.GetValue<string>("IOTEDGE_AUTHSCHEME")}");
            Console.WriteLine($"IOTEDGE_MODULEGENERATIONID={configuration.GetValue<string>("IOTEDGE_MODULEGENERATIONID")}");
            Console.WriteLine($"IOTEDGE_GATEWAYHOSTNAME={configuration.GetValue<string>("IOTEDGE_GATEWAYHOSTNAME")}");

            TransportType transportType = configuration.GetValue("ClientTransportType", TransportType.Amqp_Tcp_Only);
            Console.WriteLine($"Using transport {transportType.ToString()}");

            await InitModuleClient(transportType);

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            await WhenCancelled(cts.Token);
            return 0;
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        static async Task InitModuleClient(TransportType transportType)
        {
            ITransportSettings[] GetTransportSettings()
            {
                switch (transportType)
                {
                    case TransportType.Mqtt:
                    case TransportType.Mqtt_Tcp_Only:
                    case TransportType.Mqtt_WebSocket_Only:
                        return new ITransportSettings[] { new MqttTransportSettings(transportType) };
                    default:
                        return new ITransportSettings[] { new AmqpTransportSettings(transportType) };
                }
            }
            ITransportSettings[] settings = GetTransportSettings();

            ModuleClient moduleClient = await ModuleClient.CreateFromEnvironmentAsync(settings).ConfigureAwait(false);
            await moduleClient.OpenAsync().ConfigureAwait(false);
            await moduleClient.SetMethodHandlerAsync("HelloWorldMethod", HelloWorldMethod, null).ConfigureAwait(false);

            Console.WriteLine("Successfully initialized module client.");
        }

        static Task<MethodResponse> HelloWorldMethod(MethodRequest methodRequest, object userContext)
        {
            Console.WriteLine("Received direct method call...");
            return Task.FromResult(new MethodResponse((int)HttpStatusCode.OK));
        }
    }
}
