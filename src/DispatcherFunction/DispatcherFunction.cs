using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using StackExchange.Redis;
using Streamer.Common.Models;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;

namespace DispatcherFunction
{
    public static class DispatcherFunction
    {
        private static string RedisPrefix = "streamer:";
        private static IConfigurationRoot Config;
        private static string RedisConnectionString => Config["RedisConnectionString"];
        private static string ApiBaseAddress => Config["ApiBaseAddress"];
        private static readonly Lazy<ConnectionMultiplexer> LazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(RedisConnectionString));
        private static ConnectionMultiplexer RedisConnection => LazyConnection.Value;

        [FunctionName("DispatcherFunction")]
        public static async Task Run([EventHubTrigger("final-stream", Connection = "incomingEventHub")]
                EventData[] messages, 
                ILogger log,
                ExecutionContext context)
        {
            Config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            log.LogInformation($"{messages.Length} Events received.");

            var stopwatch = Stopwatch.StartNew();

            // let's parse the points
            var points = messages
                .Select(x => SafelyConvertToDataPoint(x.Body.Array, log))
                .AsParallel()
                .Where(x => x.point != null)
                .ToList()
                .OrderBy(x => x.point.Timestamp)
                .ToList();

            log.LogMetric($"Converted messages: {points.Count} / {messages.Length}", stopwatch.ElapsedMilliseconds);

            var pointsPerPlayer = points.GroupBy(x => x.point.Key);
            foreach (var item in pointsPerPlayer)
            {
                await ProcessPlayerAsync(item.Key, item.ToArray(), log);
            }

            stopwatch.Stop();

            log.LogMetric($"Finished processing messages: {points.Count} / {messages.Length}", stopwatch.ElapsedMilliseconds);
        }


        private static async Task ProcessPlayerAsync(string playerId, (DataPoint point, string strRepresentation)[] messages, ILogger log)
        {

            var cache = RedisConnection.GetDatabase();
            var queueKey = $"{RedisPrefix}player:{playerId}:queue";
            var startKey = $"{RedisPrefix}player:{playerId}:start";

            // check if we have a start
            var start = await cache.StringGetAsync(startKey);

            if (start == RedisValue.Null)
            {
                await cache.StringSetAsync(startKey, messages[0].point.Timestamp.Ticks, TimeSpan.FromDays(1));
                start = messages[0].point.Timestamp.Ticks;
            }

            // we can try the processing
            var startTimeStamp = new DateTime((long)start);
            var pushTime = false;
            foreach (var (px, kx) in messages)
            {
                if ((px.Timestamp - startTimeStamp).TotalSeconds >= 1)
                {
                    // start the countdown! 
                    await cache.StringSetAsync(startKey, px.Timestamp.Ticks);
                    pushTime = true;
                    startTimeStamp = px.Timestamp;
                }
            }

            // put them into the redis queue
            await cache.ListRightPushAsync(queueKey, messages.Select(x => (RedisValue)x.strRepresentation).ToArray());

            if (pushTime)
            {
                // call push time
                log.LogInformation("Pushing time!");
                await PushTimeAsync(playerId, log);
            }
        }

        private static async Task PushTimeAsync(string playerId, ILogger log)
        {
            // get the playerId queue and stuff
            var cache = RedisConnection.GetDatabase();

            var queueKey = $"{RedisPrefix}player:{playerId}:queue";


            DateTime? start = null;

            // get stuff from the queue into the buffer
            var buffer = new List<DataPoint>();
            while (true)
            {
                var value = await cache.ListLeftPopAsync(queueKey);

                if (value == RedisValue.Null)
                {
                    throw new Exception($"We've run out of queue for key: {queueKey}");
                };

                var point = JsonConvert.DeserializeObject<DataPoint>(value);
                if (start == null) start = point.Timestamp;

                buffer.Add(point);
                if ((point.Timestamp - start.Value).TotalSeconds >= 1)
                {
                    // we can process, and stop whatever we're doing
                    break;
                }
            }

            // at this point we have the object, let's average the values for the second
            var first = buffer.First();
            var countOfFields = first.Values.Count;

            var allValues = new Dictionary<string, string>();
            for (var i = 0; i < countOfFields; i++)
            {
                // TODO: this is horrible, but is a quick way to fix the errors
                var value = buffer.Average(x => InternalParse(x.Values[i])).ToString();
                allValues.Add(first.Names[i], value);
            }

            var o = new
            {
                ts = first.Timestamp,
                deviceid = first.DeviceId,
                sessionid = first.SessionId,
                sessionstart = "",
                allvalues = allValues
            };

            await PostToApi(playerId, o, log);

        }

        private static async Task PostToApi(string playerId, dynamic dp, ILogger log)
        {
            var payload = JsonConvert.SerializeObject(dp);
            log.LogInformation($"Row for {playerId}: {payload}");

            // Getting ready to post to API

            //using (var http = new HttpClient())
            //{
            //    http.BaseAddress = new Uri(ApiBaseAddress);
            //    var result = await http.PostAsync("/api/some/endpoint", new StringContent(payload));
            //    if (!result.IsSuccessStatusCode)
            //    {
            //        var resultContent = await result.Content.ReadAsStringAsync();
            //        log.LogCritical($"Unsuccessful post to api for PlayerId: {playerId} --> {result.StatusCode} | {result.ReasonPhrase} | {resultContent}");
            //    }
            //}
        }

        private static decimal InternalParse(string incoming)
        {
            decimal result;
            if (!decimal.TryParse(incoming, out result))
                return 0;
            return result;
        }

        private static (DataPoint point, string message) SafelyConvertToDataPoint(byte[] data, ILogger log)
        {
            try
            {
                var str = Encoding.UTF8.GetString(data);
                var point = JsonConvert.DeserializeObject<DataPoint>(str);
                // generate the key
                // this is a total hack, but we *assume* the value for player id is second in the array
                var key = $"{point.SessionId}:{point.Values[1]}";
                point.Key = key;
                return (point, str);
            }
            catch (Exception e)
            {
                log.LogCritical("Could not process a message ({1}) due to exception: {0}",
                    e.Message,
                    Encoding.UTF8.GetString(data));
                return (null, null);
            }
        }
    }
}
