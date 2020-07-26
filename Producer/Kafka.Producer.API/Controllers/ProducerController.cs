using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Kafka.Producer.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProducerController : ControllerBase
    {
        [HttpPost]
        [ProducesResponseType(typeof(string), 201)]
        [ProducesResponseType(400)]
        [ProducesResponseType(500)]
        public IActionResult Post([FromQuery] string message)
        {
            return Created("", SendMessageKafka(message));
        }

        private string SendMessageKafka(string message)
        {

            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {

                    var sendResult = producer
                                        .ProduceAsync("fila_pedido", new Message<Null, string> { Value = message })
                                            .GetAwaiter()
                                                .GetResult();

                }
                catch (ProduceException<Null, string> ex)
                {

                    Console.WriteLine($"Delivery failed: { ex.Error.Reason}");
                }
            }

            return string.Empty;
        }
    }
}