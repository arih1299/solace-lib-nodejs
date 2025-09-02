const solace = require('solclientjs');
const { 
    SOLACE_URL, 
    SOLACE_VPN, 
    SOLACE_USERNAME, 
    SOLACE_PASSWORD, 
    SOLACE_CLIENT_NAME 
} = require('./config');

class SolaceClient {
    constructor() {
        this.session = null;
        this.messageConsumers = new Map();
        this.isConnected = false;
        
        // Initialize the Solace API. It's a one-time operation.
        const factoryProps = new solace.SolclientFactoryProperties();
        factoryProps.profile = solace.SolclientFactoryProfiles.version10;
        solace.SolclientFactory.init(factoryProps);
    }

    /**
     * Making connection to Solace Broker.
     * 
     * @returns {void}
     */
    async connect() {
        return new Promise((resolve, reject) => {
            this.session = solace.SolclientFactory.createSession({
                url: SOLACE_URL,
                vpnName: SOLACE_VPN,
                userName: SOLACE_USERNAME,
                password: SOLACE_PASSWORD,
                clientName: SOLACE_CLIENT_NAME
            });

            this.session.on(solace.SessionEventCode.UP_NOTICE, () => {
                console.log('Solace session connected successfully.');
                this.isConnected = true;
                resolve();
            });

            this.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, (event) => {
                console.error('Solace connection failed: ', event.infoStr);
                this.session = null;
                this.isConnected = false;
                reject(new Error(event.infoStr));
            });

            this.session.on(solace.SessionEventCode.DISCONNECTED, () => {
                console.log('Solace session disconnected.');
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

    /**
     * Stop connection to Solace Broker and release all resources.
     * 
     * @returns {void}
     */
    disconnect() {
        if (this.session) {
            this.stopConsumingAll();
            this.session.dispose();
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
            //console.log(`Received message from queue ${queueName}: ${payload}`);
            
            // Pass the message to the provided handler
            messageHandler(payload);
            
            // Acknowledge the message to remove it from the queue
            message.acknowledge();
        });

        messageConsumer.on(solace.MessageConsumerEventName.UP, () => {
            console.log(`Message consumer for queue ${queueName} is ready.`);
        });

        messageConsumer.on(solace.MessageConsumerEventName.DOWN, () => {
            console.log(`Message cosumer for queue ${queueName} is down.`);
        });

        messageConsumer.on(solace.MessageConsumerEventName.DOWN_ERROR, (error) => {
            console.error(`Message cosumer for queue ${queueName} down error:`, error);
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
     * @returns {void}
     */
    stopConsuming(queueName) {
        const consumer = this.messageConsumers.get(queueName);
        if (consumer) {
            consumer.dispose();
            this.messageConsumers.delete(queueName);
            console.log(`Stopped consuming from queue: ${queueName}`);
        }
    }

    /**
     * Stop consuming messages from all queues
     * @returns {void}
     */
    stopConsumingAll() {
        if (this.messageConsumers.size > 0) {
            console.log(`Stopping ${this.messageConsumers.size} message consumers...`);
            this.messageConsumers.forEach(c => this.stopConsuming(c));
        }
    }
}

module.exports = SolaceClient;