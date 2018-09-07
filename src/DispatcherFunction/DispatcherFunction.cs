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
using System.Net.Http;
using System.Net.Http.Headers;
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
        private static string ApiRoute => Config["ApiRoute"];
        private static readonly Lazy<ConnectionMultiplexer> LazyConnection = new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(RedisConnectionString));
        private static ConnectionMultiplexer RedisConnection => LazyConnection.Value;
        //TODO: should we potentially shorten the time to expiry? 24 hrs seems more than enough, maybe something like 6-8 hours?
        //currently the data in redis is absolutely useless and there is no re-use of the data after the initial post to the api
        private static TimeSpan RedisExpiryTime => TimeSpan.FromHours(8);

        [FunctionName("DispatcherFunction")]
        public static async Task Run([EventHubTrigger("livedata", Connection = "incomingEventHub")]
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
            var queueKey = QueueKey(playerId);
            var startKey = StartKey(playerId);

            await SetSessionStartValueIfNeedsBe(messages[0].point.SessionId, messages[0].point.Timestamp.Ticks, RedisExpiryTime);
            var start = await GetStartAndSetIfNeedsBe(cache, startKey, messages[0].point.Timestamp.Ticks);

            // we can try the processing
            var startTimeStamp = new DateTime(start);
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
                await PushTimeAsync(playerId, log);
            }
        }

        private static async Task<long> GetStartAndSetIfNeedsBe(IDatabase cache, string key, long ticks)
        {
            var startVal = await cache.StringGetAsync(key);
            if (startVal == RedisValue.Null)
            {
                startVal = ticks;
                await cache.StringSetAsync(key, startVal, RedisExpiryTime);
            }
            return (long)startVal;
        }

        private static async Task SetSessionStartValueIfNeedsBe(string sessionId, long ticks, TimeSpan expiryTime)
        {
            var cache = RedisConnection.GetDatabase();
            var key = SessionStartKey(sessionId);
            if (await cache.StringGetAsync(key) == RedisValue.Null)
            {
                await cache.StringSetAsync(key, ticks, expiryTime);
            }
        }

        private static async Task PushTimeAsync(string playerId, ILogger log)
        {
            log.LogInformation("Pushing time!");

            // get the playerId queue and stuff
            var cache = RedisConnection.GetDatabase();

            var queueKey = QueueKey(playerId);

            DateTime? start = null;

            // get stuff from the queue into the buffer
            var buffer = new List<DataPoint>();
            while (true)
            {
                var value = await cache.ListLeftPopAsync(queueKey);

                if (value == RedisValue.Null)
                {
                    log.LogCritical($"We've run out of queue for key: {queueKey}");
                    return;
                    //TODO previously we were throwing an exception here, need to confirm that not throwing is better.
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

            var allValues = new Dictionary<string, decimal>();
            for (var i = 0; i < countOfFields; i++)
            {
                // TODO: this is horrible, but is a quick way to fix the errors
                var value = buffer.Average(x => InternalParse(x.Values[i]));
                allValues.Add(first.Names[i], value);
            }

            var sessionStartTicks = await cache.StringGetAsync(SessionStartKey(first.SessionId));
            var sessionStart = new DateTime((long) sessionStartTicks);

            var o = new
            {
                ts = first.Timestamp,
                deviceid = first.DeviceId,
                sessionid = first.SessionId,
                sessionstart = sessionStart.ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'"),
                allvalues = allValues
            };

            await PostToApi(playerId, JsonConvert.SerializeObject(o), log);

        }

        private static async Task PostToApi(string playerId, string dp, ILogger log)
        {
            var payload = "{\"input\":[" + dp + "]}";

            log.LogInformation($"posting Row for {playerId}: {payload} to {ApiBaseAddress}{ApiRoute}");

            using (var http = new HttpClient())
            {

                http.BaseAddress = new Uri(ApiBaseAddress);
                http.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));//ACCEPT header
                var payloadContent = new StringContent(payload);
                payloadContent.Headers.ContentType = new MediaTypeHeaderValue("application/json");
                var result = await http.PostAsync(ApiRoute, payloadContent);
                if (result.IsSuccessStatusCode)
                {
                    //TODO: do we need to do anything on success? cleanup in redis?
                    log.LogInformation($"Successful post to api for PlayerId: {playerId}");
                }
                else
                {
                    var resultContent = await result.Content.ReadAsStringAsync();
                    log.LogCritical($"Unsuccessful post to api for PlayerId: {playerId} --> {result.StatusCode} | {result.ReasonPhrase} | {resultContent}");
                }
            }
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

        private static string StartKey(string playerId) => $"{RedisPrefix}player:{playerId}:start";
        private static string QueueKey(string playerId) => $"{RedisPrefix}player:{playerId}:queue";
        private static string SessionStartKey(string sessionId) => $"{RedisPrefix}session-start:{sessionId}";
    }
}
