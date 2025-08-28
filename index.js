const solace = require('solclientjs');

class SolaceClient {
    constructor(brokerConfig) {
        this.brokerConfig = brokerConfig;
        this.session = null;
        this.messageConsumers = new Map();
        this.isConnected = false;
        
        // Initialize the Solace API. It's a one-time operation.
        solace.SolclientFactory.init({
            url: this.brokerConfig.url,
            vpnName: this.brokerConfig.vpn,
            userName: this.brokerConfig.username,
            password: this.brokerConfig.password
        });
    }

    async connect() {
        return new Promise((resolve, reject) => {
            this.session = solace.SolclientFactory.createSession(this.brokerConfig);

            this.session.on(solace.SessionEventCode.UP_NOTICE, () => {
                console.log('Solace session connected successfully.');
                this.isConnected = true;
                resolve();
            });

            this.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, (event) => {
                console.error('Connection failed:', event.infoStr);
                this.session = null;
                this.isConnected = false;
                reject(new Error(event.infoStr));
            });

            this.session.on(solace.SessionEventCode.DISCONNECTED, () => {
                console.log('Session disconnected.');
                this.session = null;
                this.isConnected = false;
            });

            try {
                this.session.connect();
            } catch (error) {
                reject(error);
            }
        });
    }

    disconnect() {
        if (this.session) {
            this.session.disconnect();
        }
        console.log('Disconnected from Solace broker.');
    }

    /**
     * Publishes a message to a Solace topic.
     * @param {string} topicName The topic to publish to.
     * @param {string} message The message payload.
     */
    publish(topicName, message) {
        if (!this.session || !this.isConnected) {
            throw new Error('Solace session is not connected.');
        }

        const topic = solace.SolclientFactory.createTopicDestination(topicName);
        const msg = solace.SolclientFactory.createMessage();
        msg.setDestination(topic);
        msg.setBinaryAttachment(message);
        msg.setDeliveryMode(solace.MessageDeliveryModeType.PERSISTENT);

        try {
            this.session.send(msg);
            console.log(`Published message to topic: ${topicName}`);
        } catch (error) {
            console.error('Failed to publish message:', error);
            throw error;
        }
    }

    /**
     * Consumes messages from a Solace queue.
     * @param {string} queueName The queue to consume from.
     * @param {Function} messageHandler The handler function for incoming messages.
     * @returns {Promise<void>}
     */
    async consume(queueName, messageHandler) {
        if (!this.session || !this.isConnected) {
            throw new Error('Solace session is not connected.');
        }

        if (this.messageConsumers.has(queueName)) {
            console.log(`Already consuming from queue: ${queueName}`);
            return;
        }

        const messageConsumer = this.session.createMessageConsumer({
            queueDescriptor: {
                name: queueName,
                type: solace.QueueType.QUEUE
            },
            acknowledgeMode: solace.MessageConsumerAcknowledgeMode.CLIENT
        });

        messageConsumer.on(solace.MessageConsumerEventName.MESSAGE, (message) => {
            const payload = message.getBinaryAttachment();
            console.log(`Received message from queue ${queueName}: ${payload}`);
            
            // Pass the message to the provided handler
            messageHandler(payload);
            
            // Acknowledge the message to remove it from the queue
            message.acknowledge();
        });

        messageConsumer.on(solace.MessageConsumerEventName.UP, () => {
            console.log(`Message consumer for queue ${queueName} is ready.`);
        });

        messageConsumer.on(solace.MessageConsumerEventName.CONNECT_FAILED_ERROR, (error) => {
            console.error(`Failed to connect to queue ${queueName}:`, error);
        });

        return new Promise((resolve, reject) => {
            try {
                messageConsumer.connect();
                this.messageConsumers.set(queueName, messageConsumer);
                resolve();
            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * Stops consuming messages from a specific queue.
     * @param {string} queueName The queue to stop consuming from.
     */
    stopConsuming(queueName) {
        const consumer = this.messageConsumers.get(queueName);
        if (consumer) {
            consumer.dispose();
            this.messageConsumers.delete(queueName);
            console.log(`Stopped consuming from queue: ${queueName}`);
        }
    }
}

module.exports = SolaceClient;
