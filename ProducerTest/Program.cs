using Metrics;
using RdKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ProducerTest
{
    class Program
    {
        const string topicName = "perf-topic";
        const string consumerGroupName = "perf-consumer";

        static string brokers;

        static void Main(string[] args)
        {
            brokers = args.Length == 0 ? "192.168.33.50:9092" : args[0];

            Console.WriteLine("{0}: Topic - {1}", DateTime.Now.ToLongTimeString(), topicName);
            Console.WriteLine("{0}: Consumer group - {1}", DateTime.Now.ToLongTimeString(), consumerGroupName);

            var tokenSource = new CancellationTokenSource();

            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            StartProducer(tokenSource);

            var handle = new AutoResetEvent(false);
            var cancelCount = 0;
            Console.CancelKeyPress += (s, e) =>
            {
                cancelCount++;
                if (cancelCount == 2) return;

                tokenSource.Cancel();
                handle.Set();

                e.Cancel = true;
            };
            handle.WaitOne();

            Thread.Sleep(5000);

            WritePublished();

            Environment.Exit(0);
        }

        static List<DeliveryReport> reports = new List<DeliveryReport>();
        private static Thread StartProducer(CancellationTokenSource tokenSource)
        {
            var timer = Metric.Timer("Published", Unit.Events);

            var thread = new Thread(() =>
            {
                var topicConfig = new TopicConfig();
                topicConfig["request.required.acks"] = "0"; // Don't require an ack from the broker.
                var config = new Config
                {
                    GroupId = consumerGroupName,
                    EnableAutoCommit = true,
                    StatisticsInterval = TimeSpan.FromSeconds(10),
                    DefaultTopicConfig = topicConfig
                };
                config["socket.blocking.max.ms"] = "1"; // Maximum time a broker socket operation may block.
                config["queue.buffering.max.ms"] = "1"; // Maximum time to buffer data when using async mode.

                using (var publisher = new Producer(config, brokers))
                using (var topic = publisher.Topic(topicName))
                {
                    while (!tokenSource.IsCancellationRequested)
                    {
                        Thread.Sleep(1000);
                        for (var i = 0; i < 100; i++)
                        {
                            var ticks = DateTime.UtcNow.Ticks;
                            topic.Produce(Encoding.UTF8.GetBytes(ticks.ToString()), partition: (int)(ticks % 2))
                                .ContinueWith(task =>
                                {
                                    if (task.Exception != null)
                                    {
                                        Console.WriteLine("{0}: Error publishing message - {1}", DateTime.Now.ToLongTimeString(), task.Exception);
                                        return;
                                    }

                                    timer.Record((DateTime.UtcNow.Ticks - ticks) / 10000, TimeUnit.Milliseconds);
                                    reports.Add(task.Result);
                                });
                        }
                    }
                    Console.WriteLine("Producer cancelled.");
                    Thread.CurrentThread.Abort();
                }
            });
            thread.Start();

            return thread;
        }

        private static void WritePublished()
        {
            Console.WriteLine();
            Console.WriteLine("First Published -");
            reports.GroupBy(r => r.Partition)
                .Select(g => new { Partition = g.Key, Offset = g.Min(r => r.Offset) })
                .ToList()
                .ForEach(g => Console.WriteLine("P:{0} O:{1}", g.Partition, g.Offset));

            Console.WriteLine("Last Published -");
            reports.GroupBy(r => r.Partition)
                .Select(g => new { Partition = g.Key, Offset = g.Max(r => r.Offset) })
                .ToList()
                .ForEach(g => Console.WriteLine("P:{0} O:{1}", g.Partition, g.Offset));

            Console.WriteLine("Total - " + reports.Count);
        }
    }
}
