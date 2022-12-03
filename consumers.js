const { Kafka } = require('kafkajs');

async function test() {

  const groupId = process.argv[2];

  const kafka = new Kafka({
    clientId: 'kafka-nodejs',
    brokers: ['localhost:9092'],
  });

  const consumer = kafka.consumer({ 
    groupId: `email.send-consumerGroup-${groupId}` 
  });

  await consumer.connect();
  
  await consumer.subscribe({ topic: 'email.send', fromBeginning: false });

  // Handle the message received to this consumer for the subscrived topics
  await consumer.run({
    partitionsConsumedConcurrently: 3, // https://kafka.js.org/docs/consuming#a-name-concurrent-processing-a-partition-aware-concurrency
    eachMessage: async ({ topic, partition, message }) => {
      
      console.log(
        new Date(),
        { 
          topic,
          partition,
          value: JSON.parse(message.value) 
        }
      )

      if (topic === 'email.send') {
        // Send the email
      }
    },
  });
}

test();
