using Metrics;
using RdKafka;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsumerTest
{
    class Program
    {
        const string topicName = "perf-topic";
        const string consumerGroupName = "perf-consumer";

        static string brokers;

        static int Main(string[] args)
        {
            brokers = args.Length == 0 ? "192.168.33.50:9092" : args[0];

            Console.WriteLine("{0}: Topic - {1}", DateTime.Now.ToLongTimeString(), topicName);
            Console.WriteLine("{0}: Consumer group - {1}", DateTime.Now.ToLongTimeString(), consumerGroupName);

            var tokenSource = new CancellationTokenSource();

            var task = StartPollingConsumer(topicName, tokenSource);

            Metric.Config.WithReporting(r => r.WithConsoleReport(TimeSpan.FromSeconds(5)));

            task.Wait();
            Thread.Sleep(5000);

            WriteReceived();

            return 0;
        }

        static List<Message> receivedMessages = new List<Message>();
        private static Task StartPollingConsumer(string topicName, CancellationTokenSource tokenSource)
        {
            var timer = Metric.Timer("Received", Unit.Events);

            var handle = new AutoResetEvent(false);

            var config = new Config { GroupId = consumerGroupName, EnableAutoCommit = true, StatisticsInterval = TimeSpan.FromSeconds(10) };
            var consumer = new Consumer(config, brokers);
            var cancelCount = 0;
            Console.CancelKeyPress += (sender, args) =>
            {
                cancelCount++;
                if (cancelCount >= 2) return;

                Console.WriteLine("Shutting down... Press Ctrl+C again to force quit, but please wait a little while first.");

                tokenSource.Cancel();
                args.Cancel = true;
            };

            Action action = () =>
            {

                consumer.OnPartitionsAssigned += (obj, partitions) =>
                {
                    Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                    consumer.Assign(partitions);
                    handle.Set();
                };

                consumer.OnPartitionsRevoked += (obj, partitions) =>
                {
                    Console.WriteLine($"Revoked partitions: [{string.Join(", ", partitions)}]");
                    consumer.Unassign();
                };

                consumer.OnOffsetCommit += (obj, commit) =>
                {
                    if (commit.Error != ErrorCode.NO_ERROR && commit.Error != ErrorCode._NO_OFFSET)
                    {
                        Console.WriteLine($"Failed to commit offsets: {commit.Error}");
                    }
                };

                //var messageAndErrorType = typeof(MessageAndError);
                //var errorMember = messageAndErrorType.GetField("Error", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
                //var messageMember = messageAndErrorType.GetField("Message", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);

                while (!tokenSource.IsCancellationRequested)
                {
                    var result = consumer.Consume(TimeSpan.FromMilliseconds(1000));
                    var time = DateTime.UtcNow.Ticks;
                    if (!result.HasValue) continue;

                    var value = result.Value;
                    var error = value.Error; // (ErrorCode)errorMember.GetValue(value);
                    var msg = value.Message; // (Message)messageMember.GetValue(value);

                    if (error == ErrorCode._PARTITION_EOF) continue;

                    if (error != ErrorCode.NO_ERROR)
                    {
                        Console.Error.WriteLine(error);
                        continue;
                    }

                    if (msg.Payload == null || msg.Payload.Length == 0)
                    {
                        Console.WriteLine("no payload...");
                        continue;
                    }

                    var diff = (time - long.Parse(Encoding.UTF8.GetString(msg.Payload))) / 10000;
                    timer.Record(diff, TimeUnit.Milliseconds);

                    receivedMessages.Add(msg);
                }

                Console.WriteLine("Shutting down consumer.");
                consumer.Unsubscribe();
                consumer.Dispose();
            };

            consumer.Subscribe(new List<string> { topicName });
            var task = Task.Run(action);

            Console.WriteLine("Waiting for partitions...");
            handle.WaitOne();

            return task;
        }

        private static void WriteReceived()
        {
            Console.WriteLine();
            Console.WriteLine("First Received -");
            receivedMessages.GroupBy(r => r.Partition)
                .Select(g => new { Partition = g.Key, Offset = g.Min(r => r.Offset) })
                .ToList()
                .ForEach(g => Console.WriteLine("P:{0} O:{1}", g.Partition, g.Offset));

            Console.WriteLine("Last Received -");
            receivedMessages.GroupBy(r => r.Partition)
                .Select(g => new { Partition = g.Key, Offset = g.Max(r => r.Offset) })
                .ToList()
                .ForEach(g => Console.WriteLine("P:{0} O:{1}", g.Partition, g.Offset));

            Console.WriteLine("Total - " + receivedMessages.Count);
        }
    }
}
