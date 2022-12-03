const { Kafka } = require('kafkajs');

async function test() {
  const kafka = new Kafka({
    clientId: 'kafka-nodejs-intro',
    brokers: ['localhost:9092'],
  });

  // Initialize the Kafka producer and consumer
  const producer = kafka.producer();
  await producer.connect();

  let messageCount = process.argv[2] || 10;
  let messages = [];

  for (let i = 0; i < messageCount; i++) {
    // Send an event to the email.send topic
    let value = { 
      subject: `Message number ${i+1}`,
      to: 'abc@example.com',
      template: 'forgot.password',
      data: {
        otp: '1234',
        name: 'John Doe',
        link: 'http://example.com'
      }
    };
    
    messages.push({
      value: JSON.stringify(value)
    })
  }

  producer.send({
    topic: 'email.send',
    messages: messages,
  });

  await producer.disconnect();
}

test();
