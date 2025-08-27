const SolaceClient = require('./index');

const config = {
    // Replace with your Solace broker details
    url: 'tcp://your-solace-broker-url:55555',
    vpnName: 'your-message-vpn',
    userName: 'your-username',
    password: 'your-password'
};

const topicName = 'my/sample/topic';
const queueName = 'my-sample-queue';

const client = new SolaceClient(config);

async function run() {
    try {
        await client.connect();

        // 🚀 Publishing an event
        console.log('\n--- Publishing message to topic ---');
        client.publish(topicName, 'Hello, Solace! This is a test message.');

        // 👂 Consuming events from a queue
        console.log('\n--- Starting consumer for queue ---');
        await client.consume(queueName, (message) => {
            console.log(`\nReceived message from handler: ${message}`);
        });

        // Add a subscription for the consumer to receive messages published to the topic
        client.session.subscribe(
            solace.SolclientFactory.createTopicDestination(topicName),
            true, // Use 'true' to subscribe to the topic on the broker
            topicName,
            10000
        );
        
        // Wait a few seconds to demonstrate message reception
        setTimeout(() => {
            client.disconnect();
        }, 10000);

    } catch (error) {
        console.error('An error occurred:', error);
    }
}

run();
