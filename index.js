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
        this.requestHandlers = new Map();
        this.pendingRequests = new Map();
        this.replyConsumer = null;
        this.isConnected = false;
        
        // Initialize the Solace API. It's a one-time operation.
        const factoryProps = new solace.SolclientFactoryProperties();
        factoryProps.profile = solace.SolclientFactoryProfiles.version10;
        solace.SolclientFactory.init(factoryProps);
    }

    /**
     * Making connection to Solace Broker.
     * 
     * @returns {Promise<void>}
     */
    async connect() {
        return new Promise((resolve, reject) => {
            this.session = solace.SolclientFactory.createSession({
                url: SOLACE_URL,
                vpnName: SOLACE_VPN,
                userName: SOLACE_USERNAME,
                password: SOLACE_PASSWORD,
                clientName: SOLACE_CLIENT_NAME || 'SolaceNodeClient',
                generateSendTimestamps: true,
                generateReceiveTimestamps: true,
                generateSenderID: true,
                reconnectRetries: 3,
                connectRetries: 3,
                connectTimeoutInMsecs: 10000,
                readTimeoutInMsecs: 10000
            });

            this.session.on(solace.SessionEventCode.UP_NOTICE, () => {
                console.log('Connected to Solace broker');
                this.isConnected = true;
                resolve();
            });

            this.session.on(solace.SessionEventCode.CONNECT_FAILED_ERROR, (event) => {
                console.error('Connection failed to Solace broker:', event.infoStr);
                this.isConnected = false;
                reject(new Error(event.infoStr));
            });

            this.session.on(solace.SessionEventCode.DOWN_ERROR, (event) => {
                console.error('Session down:', event.infoStr);
                this.isConnected = false;
            });

            this.session.on(solace.SessionEventCode.DISCONNECTED, () => {
                console.log('Disconnected from Solace broker');
                this.isConnected = false;
                // Clean up pending requests on disconnect
                this._cleanupPendingRequests();
            });

            try {
                this.session.connect();
            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * Disconnect from Solace broker and cleanup resources
     * 
     * @returns {Promise<void>}
     */
    async disconnect() {
        return new Promise((resolve) => {
            if (this.session) {
                // Clean up all consumers
                for (const [, consumer] of this.messageConsumers) {
                    try {
                        consumer.disconnect();
                    } catch (error) {
                        console.warn('Error disconnecting consumer:', error.message);
                    }
                }
                this.messageConsumers.clear();

                // Clean up reply consumer
                if (this.replyConsumer) {
                    try {
                        this.replyConsumer.disconnect();
                    } catch (error) {
                        console.warn('Error disconnecting reply consumer:', error.message);
                    }
                    this.replyConsumer = null;
                }

                // Clean up pending requests
                this._cleanupPendingRequests();

                this.session.on(solace.SessionEventCode.DISCONNECTED, () => {
                    this.isConnected = false;
                    resolve();
                });

                this.session.disconnect();
            } else {
                resolve();
            }
        });
    }

    /**
     * Publish a message to a topic
     * 
     * @param {string} topicName - The topic to publish to
     * @param {string|Object} messageContent - The message content
     * @returns {Promise<void>}
     */
    async publishMessage(topicName, messageContent) {
        if (!this.session) {
            throw new Error('Session not established');
        }

        const message = solace.SolclientFactory.createMessage();
        const topic = solace.SolclientFactory.createTopicDestination(topicName);
        
        message.setDestination(topic);
        
        if (typeof messageContent === 'object') {
            message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, JSON.stringify(messageContent)));
        } else {
            message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, messageContent));
        }

        try {
            this.session.send(message);
            console.log(`Message published to topic: ${topicName}`);
        } catch (error) {
            console.error('Failed to publish message:', error);
            throw error;
        }
    }

    /**
     * Subscribe to a queue and start consuming messages
     * 
     * @param {string} queueName - The queue to subscribe to
     * @param {Function} messageHandler - Function to handle received messages
     * @returns {Promise<void>}
     */
    async subscribeToQueue(queueName, messageHandler) {
        if (!this.session) {
            throw new Error('Session not established');
        }

        return new Promise((resolve, reject) => {
            const messageConsumer = this.session.createMessageConsumer({
                queueDescriptor: { name: queueName, type: solace.QueueType.QUEUE },
                acknowledgeMode: solace.MessageConsumerAcknowledgeMode.CLIENT,
                createIfMissing: true
            });

            messageConsumer.on(solace.MessageConsumerEventName.UP, () => {
                console.log(`Subscribed to queue: ${queueName}`);
                this.messageConsumers.set(queueName, messageConsumer);
                resolve();
            });

            messageConsumer.on(solace.MessageConsumerEventName.CONNECT_FAILED_ERROR, (error) => {
                console.error(`Failed to subscribe to queue: ${queueName}`, error);
                reject(error);
            });

            messageConsumer.on(solace.MessageConsumerEventName.MESSAGE, (message) => {
                try {
                    const messageBody = message.getSdtContainer().getValue();
                    messageHandler(messageBody, message);
                    message.acknowledge();
                } catch (error) {
                    console.error('Error handling message:', error);
                    message.acknowledge(); // Still acknowledge to avoid redelivery loops
                }
            });

            messageConsumer.connect();
        });
    }

    // ============= REQUEST-RESPONSE FUNCTIONALITY =============

    /**
     * Sends a request message and waits for a response using a temporary reply queue
     * 
     * @param {string} requestTopic - The topic to send the request to
     * @param {string|Object} requestMessage - The request message payload
     * @param {number} timeoutMs - Timeout in milliseconds (default: 5000)
     * @returns {Promise<string>} - Promise that resolves with the response message
     */
    async sendRequest(requestTopic, requestMessage, timeoutMs = 5000) {
        if (!this.session) {
            throw new Error('Session not established');
        }

        // Generate unique correlation ID for this request
        const correlationId = `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
        // Create temporary reply queue name
        const replyQueueName = `#P2P/QUE/tmp_reply_${correlationId}`;
        
        return new Promise((resolve, reject) => {
            let timeoutHandle;
            let replyConsumer;

            // Set up timeout
            timeoutHandle = setTimeout(() => {
                this._cleanupRequest(correlationId, replyConsumer);
                reject(new Error(`Request timeout after ${timeoutMs}ms`));
            }, timeoutMs);

            // Create temporary reply queue consumer
            replyConsumer = this.session.createMessageConsumer({
                queueDescriptor: { 
                    name: replyQueueName, 
                    type: solace.QueueType.QUEUE 
                },
                acknowledgeMode: solace.MessageConsumerAcknowledgeMode.CLIENT,
                createIfMissing: true
            });

            replyConsumer.on(solace.MessageConsumerEventName.UP, () => {
                console.log(`Reply consumer ready for correlation ID: ${correlationId}`);

                // Now send the request message
                try {
                    const message = solace.SolclientFactory.createMessage();
                    const topic = solace.SolclientFactory.createTopicDestination(requestTopic);
                    const replyToQueue = solace.SolclientFactory.createDurableQueueDestination(replyQueueName);
                    
                    message.setDestination(topic);
                    message.setReplyTo(replyToQueue);
                    message.setCorrelationId(correlationId);
                    
                    if (typeof requestMessage === 'object') {
                        message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, JSON.stringify(requestMessage)));
                    } else {
                        message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, requestMessage));
                    }

                    this.session.send(message);
                    console.log(`Request sent to topic: ${requestTopic} with correlation ID: ${correlationId}`);
                    
                    // Store request for tracking
                    this.pendingRequests.set(correlationId, {
                        resolve,
                        reject,
                        timeoutHandle,
                        replyConsumer,
                        timestamp: Date.now()
                    });

                } catch (error) {
                    clearTimeout(timeoutHandle);
                    this._cleanupRequest(correlationId, replyConsumer);
                    reject(error);
                }
            });

            replyConsumer.on(solace.MessageConsumerEventName.CONNECT_FAILED_ERROR, (error) => {
                clearTimeout(timeoutHandle);
                console.error(`Failed to create reply consumer for correlation ID: ${correlationId}`, error);
                reject(error);
            });

            replyConsumer.on(solace.MessageConsumerEventName.MESSAGE, (message) => {
                try {
                    const receivedCorrelationId = message.getCorrelationId();
                    
                    if (receivedCorrelationId === correlationId) {
                        const responseBody = message.getSdtContainer().getValue();
                        message.acknowledge();
                        
                        // Clean up and resolve
                        clearTimeout(timeoutHandle);
                        this._cleanupRequest(correlationId, replyConsumer);
                        
                        console.log(`Response received for correlation ID: ${correlationId}`);
                        resolve(responseBody);
                    } else {
                        // Not our message, acknowledge but don't process
                        message.acknowledge();
                        console.warn(`Received message with unexpected correlation ID: ${receivedCorrelationId}, expected: ${correlationId}`);
                    }
                } catch (error) {
                    message.acknowledge();
                    clearTimeout(timeoutHandle);
                    this._cleanupRequest(correlationId, replyConsumer);
                    reject(error);
                }
            });

            // Start the reply consumer
            replyConsumer.connect();
        });
    }

    /**
     * Set up a request handler for incoming requests on a specific topic
     * 
     * @param {string} requestTopic - The topic to listen for requests on
     * @param {Function} requestHandler - Function to handle requests (should return response or Promise)
     * @returns {Promise<void>}
     */
    async handleRequests(requestTopic, requestHandler) {
        if (!this.session) {
            throw new Error('Session not established');
        }

        return new Promise((resolve, reject) => {
            // Create topic subscription for requests
            const topicSubscription = solace.SolclientFactory.createTopicDestination(requestTopic);
            
            try {
                this.session.subscribe(
                    topicSubscription,
                    true, // requestConfirmation
                    requestTopic, // correlationKey
                    10000 // requestTimeout
                );

                // Set up message handler
                this.session.on(solace.SessionEventCode.MESSAGE, async (message) => {
                    const destination = message.getDestination();
                    
                    // Check if this message is for our subscribed topic
                    if (destination && destination.getName() === requestTopic) {
                        try {
                            const requestBody = message.getSdtContainer().getValue();
                            const replyTo = message.getReplyTo();
                            const correlationId = message.getCorrelationId();

                            console.log(`Request received on topic: ${requestTopic}, correlation ID: ${correlationId}`);

                            if (replyTo && correlationId) {
                                // Process the request
                                let response;
                                try {
                                    response = await requestHandler(requestBody, message);
                                } catch (handlerError) {
                                    console.error('Request handler error:', handlerError);
                                    response = { error: 'Internal server error', message: handlerError.message };
                                }

                                // Send response
                                await this._sendResponse(replyTo, correlationId, response);
                            } else {
                                console.warn('Received request without reply-to destination or correlation ID');
                            }
                        } catch (error) {
                            console.error('Error processing request:', error);
                        }
                    }
                });

                this.session.on(solace.SessionEventCode.SUBSCRIPTION_OK, (event) => {
                    if (event.correlationKey === requestTopic) {
                        console.log(`Request handler set up for topic: ${requestTopic}`);
                        this.requestHandlers.set(requestTopic, requestHandler);
                        resolve();
                    }
                });

                this.session.on(solace.SessionEventCode.SUBSCRIPTION_ERROR, (event) => {
                    if (event.correlationKey === requestTopic) {
                        console.error(`Failed to set up request handler for topic: ${requestTopic}`, event);
                        reject(new Error(event.infoStr));
                    }
                });

            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * Remove request handler for a specific topic
     * 
     * @param {string} requestTopic - The topic to stop handling requests for
     * @returns {Promise<void>}
     */
    async removeRequestHandler(requestTopic) {
        if (!this.session) {
            throw new Error('Session not established');
        }

        return new Promise((resolve, reject) => {
            const topicSubscription = solace.SolclientFactory.createTopicDestination(requestTopic);
            
            try {
                this.session.unsubscribe(
                    topicSubscription,
                    true, // requestConfirmation
                    requestTopic, // correlationKey
                    10000 // requestTimeout
                );

                this.session.on(solace.SessionEventCode.SUBSCRIPTION_OK, (event) => {
                    if (event.correlationKey === requestTopic) {
                        console.log(`Request handler removed for topic: ${requestTopic}`);
                        this.requestHandlers.delete(requestTopic);
                        resolve();
                    }
                });

                this.session.on(solace.SessionEventCode.SUBSCRIPTION_ERROR, (event) => {
                    if (event.correlationKey === requestTopic) {
                        console.error(`Failed to remove request handler for topic: ${requestTopic}`, event);
                        reject(new Error(event.infoStr));
                    }
                });

            } catch (error) {
                reject(error);
            }
        });
    }

    /**
     * Create a request-reply session optimized for high-frequency request-response operations
     * 
     * @param {Object} options - Configuration options
     * @param {string} options.requestTopic - Base topic for requests
     * @param {number} options.defaultTimeout - Default timeout for requests (default: 5000ms)
     * @param {number} options.maxConcurrentRequests - Maximum concurrent requests (default: 100)
     * @returns {Object} - Request-reply session object with optimized methods
     */
    createRequestReplySession(options = {}) {
        const {
            requestTopic,
            defaultTimeout = 5000,
            maxConcurrentRequests = 100
        } = options;

        if (!requestTopic) {
            throw new Error('requestTopic is required for request-reply session');
        }

        if (!this.session) {
            throw new Error('Session not established');
        }

        let requestCounter = 0;
        const activeRequests = new Map();

        return {
            /**
             * Send a request using the optimized session
             */
            sendRequest: async (message, timeout = defaultTimeout) => {
                if (activeRequests.size >= maxConcurrentRequests) {
                    throw new Error(`Maximum concurrent requests (${maxConcurrentRequests}) exceeded`);
                }

                const requestId = `${requestTopic}_${++requestCounter}_${Date.now()}`;
                
                try {
                    const response = await this.sendRequest(requestTopic, message, timeout);
                    return response;
                } finally {
                    activeRequests.delete(requestId);
                }
            },

            /**
             * Get session statistics
             */
            getStats: () => ({
                activeRequests: activeRequests.size,
                maxConcurrentRequests,
                totalRequestsSent: requestCounter,
                requestTopic
            }),

            /**
             * Close the request-reply session
             */
            close: () => {
                // Clean up any active requests
                for (const [requestId] of activeRequests) {
                    activeRequests.delete(requestId);
                }
                console.log(`Request-reply session closed for topic: ${requestTopic}`);
            }
        };
    }

    // ============= PRIVATE HELPER METHODS =============

    /**
     * Send a response message to the specified reply destination
     * 
     * @private
     * @param {Object} replyTo - Reply destination
     * @param {string} correlationId - Correlation ID from the original request
     * @param {string|Object} responseData - Response data
     */
    async _sendResponse(replyTo, correlationId, responseData) {
        try {
            const responseMessage = solace.SolclientFactory.createMessage();
            responseMessage.setDestination(replyTo);
            responseMessage.setCorrelationId(correlationId);
            
            if (typeof responseData === 'object') {
                responseMessage.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, JSON.stringify(responseData)));
            } else {
                responseMessage.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, responseData));
            }

            this.session.send(responseMessage);
            console.log(`Response sent for correlation ID: ${correlationId}`);
        } catch (error) {
            console.error('Failed to send response:', error);
            throw error;
        }
    }

    /**
     * Clean up a specific request
     * 
     * @private
     * @param {string} correlationId - Correlation ID of the request to clean up
     * @param {Object} replyConsumer - Reply consumer to disconnect
     */
    _cleanupRequest(correlationId, replyConsumer) {
        // Remove from pending requests
        const request = this.pendingRequests.get(correlationId);
        if (request) {
            if (request.timeoutHandle) {
                clearTimeout(request.timeoutHandle);
            }
            this.pendingRequests.delete(correlationId);
        }

        // Disconnect reply consumer
        if (replyConsumer) {
            try {
                replyConsumer.disconnect();
            } catch (error) {
                console.warn(`Error disconnecting reply consumer for correlation ID ${correlationId}:`, error.message);
            }
        }
    }

    /**
     * Clean up all pending requests
     * 
     * @private
     */
    _cleanupPendingRequests() {
        for (const [correlationId, request] of this.pendingRequests) {
            if (request.timeoutHandle) {
                clearTimeout(request.timeoutHandle);
            }
            if (request.replyConsumer) {
                try {
                    request.replyConsumer.disconnect();
                } catch (error) {
                    console.warn(`Error disconnecting reply consumer for correlation ID ${correlationId}:`, error.message);
                }
            }
            if (request.reject) {
                request.reject(new Error('Session disconnected'));
            }
        }
        this.pendingRequests.clear();
    }

    /**
     * Get connection status and session statistics
     * 
     * @returns {Object} - Status and statistics
     */
    getStatus() {
        return {
            isConnected: this.isConnected,
            activeConsumers: this.messageConsumers.size,
            activeRequestHandlers: this.requestHandlers.size,
            pendingRequests: this.pendingRequests.size,
            sessionInfo: this.session ? {
                sessionName: this.session.getSessionName(),
                capabilities: this.session.getCapabilities()
            } : null
        };
    }
}

module.exports = SolaceClient;