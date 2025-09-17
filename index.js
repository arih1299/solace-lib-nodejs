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
     * Publish a message to a topic with comprehensive Solace message properties
     * 
     * @param {string} topicName - The topic to publish to
     * @param {string|Object|Buffer} messageContent - The message content
     * @param {Object} options - Advanced message options
     * @param {boolean} options.persistent - Message persistence (default: false)
     * @param {number} options.timeToLive - Time to live in milliseconds (0 = no expiry)
     * @param {number} options.priority - Message priority (0-255, where 255 is highest)
     * @param {string} options.correlationId - Correlation ID for message tracking
     * @param {string} options.messageId - Unique message identifier
     * @param {string} options.applicationMessageId - Application-specific message ID
     * @param {string} options.senderId - Sender identification
     * @param {string} options.messageType - Application message type
     * @param {string} options.contentType - MIME content type (e.g., 'application/json')
     * @param {string} options.contentEncoding - Content encoding (e.g., 'gzip', 'base64')
     * @param {Object} options.userProperties - User-defined properties (key-value pairs)
     * @param {Object} options.replyTo - Reply-to destination (topic or queue)
     * @param {boolean} options.dmqEligible - Dead Message Queue eligibility (default: false)
     * @param {boolean} options.elidingEligible - Message eliding eligibility (default: false)
     * @param {string} options.className - SDT class name for structured data
     * @param {string} options.deliveryMode - Delivery mode ('DIRECT', 'PERSISTENT', 'NON_PERSISTENT')
     * @param {number} options.expiration - Absolute expiration time (Unix timestamp)
     * @param {string} options.httpContentType - HTTP content type for REST consumers
     * @param {string} options.partitionKey - Partition key for guaranteed messaging
     * @param {boolean} options.ackImmediately - Acknowledge immediately (default: false)
     * @param {Object} options.destination - Override destination (for advanced routing)
     * @returns {Promise<void>}
     */
    async publishMessage(topicName, messageContent, options = {}) {
        if (!this.session) {
            throw new Error('Session not established');
        }

        try {
            const message = solace.SolclientFactory.createMessage();
            
            // Set destination (topic or custom destination)
            let destination;
            if (options.destination) {
                destination = options.destination;
            } else {
                destination = solace.SolclientFactory.createTopicDestination(topicName);
            }
            message.setDestination(destination);

            // Set message content with appropriate SDT type
            this._setMessageContent(message, messageContent, options);

            // ============= CORE MESSAGE PROPERTIES =============

            // Persistence and Delivery Mode
            if (options.persistent !== undefined || options.deliveryMode) {
                let deliveryMode;
                if (options.deliveryMode) {
                    switch (options.deliveryMode.toUpperCase()) {
                        case 'DIRECT':
                            deliveryMode = solace.MessageDeliveryModeType.DIRECT;
                            break;
                        case 'PERSISTENT':
                            deliveryMode = solace.MessageDeliveryModeType.PERSISTENT;
                            break;
                        case 'NON_PERSISTENT':
                            deliveryMode = solace.MessageDeliveryModeType.NON_PERSISTENT;
                            break;
                        default:
                            throw new Error(`Invalid delivery mode: ${options.deliveryMode}`);
                    }
                } else {
                    deliveryMode = options.persistent ? 
                        solace.MessageDeliveryModeType.PERSISTENT : 
                        solace.MessageDeliveryModeType.DIRECT;
                }
                message.setDeliveryMode(deliveryMode);
            }

            // Time To Live (TTL)
            if (options.timeToLive !== undefined) {
                if (typeof options.timeToLive !== 'number' || options.timeToLive < 0) {
                    throw new Error('timeToLive must be a non-negative number');
                }
                message.setTimeToLive(options.timeToLive);
            }

            // Message Priority (0-255)
            if (options.priority !== undefined) {
                if (typeof options.priority !== 'number' || options.priority < 0 || options.priority > 255) {
                    throw new Error('priority must be a number between 0 and 255');
                }
                message.setPriority(options.priority);
            }

            // ============= MESSAGE IDENTIFICATION =============

            // Correlation ID
            if (options.correlationId) {
                message.setCorrelationId(options.correlationId);
            }

            // Application Message ID
            if (options.applicationMessageId) {
                message.setApplicationMessageId(options.applicationMessageId);
            }

            // Message ID (auto-generated if not provided)
            if (options.messageId) {
                message.setMessageId(options.messageId);
            }

            // Sender ID
            if (options.senderId) {
                message.setSenderId(options.senderId);
            }

            // ============= CONTENT METADATA =============

            // Content Type
            if (options.contentType) {
                message.setContentType(options.contentType);
            }

            // Content Encoding
            if (options.contentEncoding) {
                message.setContentEncoding(options.contentEncoding);
            }

            // HTTP Content Type (for REST consumers)
            if (options.httpContentType) {
                message.setHttpContentType(options.httpContentType);
            }

            // Message Type
            if (options.messageType) {
                message.setType(options.messageType);
            }

            // ============= ROUTING AND REPLY =============

            // Reply-To destination
            if (options.replyTo) {
                let replyToDestination;
                if (typeof options.replyTo === 'string') {
                    // Assume it's a topic unless it looks like a queue
                    if (options.replyTo.startsWith('queue:') || options.replyTo.includes('#P2P')) {
                        const queueName = options.replyTo.replace('queue:', '');
                        replyToDestination = solace.SolclientFactory.createDurableQueueDestination(queueName);
                    } else {
                        replyToDestination = solace.SolclientFactory.createTopicDestination(options.replyTo);
                    }
                } else if (options.replyTo.type && options.replyTo.name) {
                    // Custom destination object
                    if (options.replyTo.type === 'queue') {
                        replyToDestination = solace.SolclientFactory.createDurableQueueDestination(options.replyTo.name);
                    } else {
                        replyToDestination = solace.SolclientFactory.createTopicDestination(options.replyTo.name);
                    }
                } else {
                    replyToDestination = options.replyTo; // Assume it's already a destination object
                }
                message.setReplyTo(replyToDestination);
            }

            // Partition Key (for guaranteed messaging and partitioning)
            if (options.partitionKey) {
                message.setPartitionKey(options.partitionKey);
            }

            // ============= MESSAGE FLAGS =============

            // Dead Message Queue (DMQ) Eligibility
            if (options.dmqEligible !== undefined) {
                message.setDMQEligible(options.dmqEligible);
            }

            // Message Eliding Eligibility
            if (options.elidingEligible !== undefined) {
                message.setElidingEligible(options.elidingEligible);
            }

            // ============= EXPIRATION =============

            // Absolute expiration time
            if (options.expiration !== undefined) {
                if (typeof options.expiration !== 'number' || options.expiration < 0) {
                    throw new Error('expiration must be a non-negative Unix timestamp');
                }
                message.setExpiration(options.expiration);
            }

            // ============= USER PROPERTIES =============

            // User-defined properties
            if (options.userProperties && typeof options.userProperties === 'object') {
                const userPropertyMap = new solace.SDTMapContainer();
                
                for (const [key, value] of Object.entries(options.userProperties)) {
                    // Determine the appropriate SDT type based on value type
                    let sdtField;
                    if (typeof value === 'string') {
                        sdtField = solace.SDTField.create(solace.SDTFieldType.STRING, value);
                    } else if (typeof value === 'number') {
                        if (Number.isInteger(value)) {
                            sdtField = solace.SDTField.create(solace.SDTFieldType.INT64, value);
                        } else {
                            sdtField = solace.SDTField.create(solace.SDTFieldType.DOUBLE, value);
                        }
                    } else if (typeof value === 'boolean') {
                        sdtField = solace.SDTField.create(solace.SDTFieldType.BOOL, value);
                    } else if (value instanceof Date) {
                        sdtField = solace.SDTField.create(solace.SDTFieldType.INT64, value.getTime());
                    } else if (Buffer.isBuffer(value)) {
                        sdtField = solace.SDTField.create(solace.SDTFieldType.BYTEARRAY, value);
                    } else {
                        // Convert complex objects to JSON string
                        sdtField = solace.SDTField.create(solace.SDTFieldType.STRING, JSON.stringify(value));
                    }
                    
                    userPropertyMap.addField(key, sdtField);
                }
                
                message.setUserPropertyMap(userPropertyMap);
            }

            // ============= SEND MESSAGE =============

            // Send the message with optional immediate acknowledgment
            if (options.ackImmediately) {
                // For guaranteed messaging with immediate acknowledgment
                this.session.send(message);
            } else {
                this.session.send(message);
            }

            console.log(`Enhanced message published to topic: ${topicName}`, {
                persistent: options.persistent,
                ttl: options.timeToLive,
                priority: options.priority,
                correlationId: options.correlationId,
                messageType: options.messageType,
                userProperties: options.userProperties ? Object.keys(options.userProperties) : undefined
            });

        } catch (error) {
            console.error('Failed to publish enhanced message:', error);
            throw error;
        }
    }

    /**
     * Simple publish method (backwards compatibility)
     * 
     * @param {string} topicName - The topic to publish to
     * @param {string|Object} messageContent - The message content
     * @returns {Promise<void>}
     */
    async publishSimpleMessage(topicName, messageContent) {
        return this.publishMessage(topicName, messageContent, {});
    }

    /**
     * Publish a persistent message with TTL
     * 
     * @param {string} topicName - The topic to publish to
     * @param {string|Object} messageContent - The message content
     * @param {number} timeToLiveMs - Time to live in milliseconds
     * @param {number} priority - Message priority (0-255)
     * @returns {Promise<void>}
     */
    async publishPersistentMessage(topicName, messageContent, timeToLiveMs = 0, priority = 0) {
        return this.publishMessage(topicName, messageContent, {
            persistent: true,
            timeToLive: timeToLiveMs,
            priority: priority,
            dmqEligible: true
        });
    }

    /**
     * Publish a high-priority message
     * 
     * @param {string} topicName - The topic to publish to
     * @param {string|Object} messageContent - The message content
     * @param {Object} options - Additional options
     * @returns {Promise<void>}
     */
    async publishHighPriorityMessage(topicName, messageContent, options = {}) {
        return this.publishMessage(topicName, messageContent, {
            ...options,
            priority: 255,
            persistent: true,
            dmqEligible: true
        });
    }

    /**
     * Publish a message with expiration
     * 
     * @param {string} topicName - The topic to publish to
     * @param {string|Object} messageContent - The message content
     * @param {Date} expirationDate - When the message should expire
     * @param {Object} options - Additional options
     * @returns {Promise<void>}
     */
    async publishWithExpiration(topicName, messageContent, expirationDate, options = {}) {
        return this.publishMessage(topicName, messageContent, {
            ...options,
            expiration: expirationDate.getTime(),
            dmqEligible: true
        });
    }

    /**
     * Set message content with appropriate SDT type detection
     * 
     * @private
     * @param {Object} message - Solace message object
     * @param {*} content - Content to set
     * @param {Object} options - Message options
     */
    _setMessageContent(message, content, options) {
        if (Buffer.isBuffer(content)) {
            // Binary content
            message.setBinaryAttachment(content);
        } else if (typeof content === 'string') {
            // String content
            if (options.contentType === 'application/json' || options.contentType === 'text/json') {
                message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, content));
            } else {
                message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, content));
            }
        } else if (typeof content === 'object' && content !== null) {
            // Object content - serialize to JSON
            const jsonString = JSON.stringify(content);
            message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, jsonString));
            
            // Set content type if not already specified
            if (!options.contentType) {
                message.setContentType('application/json');
            }
        } else if (typeof content === 'number') {
            // Numeric content
            if (Number.isInteger(content)) {
                message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.INT64, content));
            } else {
                message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.DOUBLE, content));
            }
        } else if (typeof content === 'boolean') {
            // Boolean content
            message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.BOOL, content));
        } else {
            // Fallback to string representation
            message.setSdtContainer(solace.SDTField.create(solace.SDTFieldType.STRING, String(content)));
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
                    // Extract message content and metadata
                    const messageData = this._extractMessageData(message);
                    messageHandler(messageData.content, message, messageData.metadata);
                    message.acknowledge();
                } catch (error) {
                    console.error('Error handling message:', error);
                    message.acknowledge(); // Still acknowledge to avoid redelivery loops
                }
            });

            messageConsumer.connect();
        });
    }

    /**
     * Extract comprehensive message data including all properties
     * 
     * @private
     * @param {Object} message - Solace message object
     * @returns {Object} - Extracted message data and metadata
     */
    _extractMessageData(message) {
        const metadata = {
            messageId: message.getMessageId(),
            correlationId: message.getCorrelationId(),
            applicationMessageId: message.getApplicationMessageId(),
            senderId: message.getSenderId(),
            messageType: message.getType(),
            contentType: message.getContentType(),
            contentEncoding: message.getContentEncoding(),
            httpContentType: message.getHttpContentType(),
            deliveryMode: message.getDeliveryMode(),
            priority: message.getPriority(),
            timeToLive: message.getTimeToLive(),
            expiration: message.getExpiration(),
            dmqEligible: message.isDMQEligible(),
            elidingEligible: message.isElidingEligible(),
            partitionKey: message.getPartitionKey(),
            sendTimestamp: message.getSenderTimestamp(),
            receiveTimestamp: message.getRcvTimestamp(),
            redelivered: message.isRedelivered(),
            discardIndication: message.isDiscardIndication(),
            destination: message.getDestination()?.getName(),
            replyTo: message.getReplyTo()?.getName(),
            userProperties: this._extractUserProperties(message)
        };

        // Extract content based on type
        let content;
        try {
            if (message.getBinaryAttachment()) {
                content = message.getBinaryAttachment();
            } else if (message.getSdtContainer()) {
                const sdtContainer = message.getSdtContainer();
                content = sdtContainer.getValue();
                
                // Try to parse JSON if content type indicates JSON
                if (metadata.contentType === 'application/json' || metadata.contentType === 'text/json') {
                    try {
                        content = JSON.parse(content);
                    } catch (e) {
                        // Keep as string if JSON parsing fails
                    }
                }
            } else {
                content = null;
            }
        } catch (error) {
            console.warn('Error extracting message content:', error);
            content = null;
        }

        return { content, metadata };
    }

    /**
     * Extract user properties from message
     * 
     * @private
     * @param {Object} message - Solace message object
     * @returns {Object} - User properties object
     */
    _extractUserProperties(message) {
        try {
            const userPropertyMap = message.getUserPropertyMap();
            if (!userPropertyMap) {
                return {};
            }

            const properties = {};
            const keys = userPropertyMap.getKeys();
            
            for (const key of keys) {
                const field = userPropertyMap.getField(key);
                if (field) {
                    properties[key] = field.getValue();
                }
            }
            
            return properties;
        } catch (error) {
            console.warn('Error extracting user properties:', error);
            return {};
        }
    }

    // ============= REQUEST-RESPONSE FUNCTIONALITY =============

    /**
     * Sends a request message and waits for a response using a temporary reply queue
     * 
     * @param {string} requestTopic - The topic to send the request to
     * @param {string|Object} requestMessage - The request message payload
     * @param {number} timeoutMs - Timeout in milliseconds (default: 5000)
     * @param {Object} messageOptions - Additional message options for the request
     * @returns {Promise<string>} - Promise that resolves with the response message
     */
    async sendRequest(requestTopic, requestMessage, timeoutMs = 5000, messageOptions = {}) {
        if (!this.session) {
            throw new Error('Session not established');
        }

        // Generate unique correlation ID for this request
        const correlationId = messageOptions.correlationId || `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        
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

                // Send the request message with enhanced options
                const requestOptions = {
                    ...messageOptions,
                    correlationId: correlationId,
                    replyTo: { type: 'queue', name: replyQueueName }
                };

                this.publishMessage(requestTopic, requestMessage, requestOptions)
                    .then(() => {
                        console.log(`Enhanced request sent to topic: ${requestTopic} with correlation ID: ${correlationId}`);
                        
                        // Store request for tracking
                        this.pendingRequests.set(correlationId, {
                            resolve,
                            reject,
                            timeoutHandle,
                            replyConsumer,
                            timestamp: Date.now()
                        });
                    })
                    .catch((error) => {
                        clearTimeout(timeoutHandle);
                        this._cleanupRequest(correlationId, replyConsumer);
                        reject(error);
                    });
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
                        // Extract full message data
                        const messageData = this._extractMessageData(message);
                        message.acknowledge();
                        
                        // Clean up and resolve
                        clearTimeout(timeoutHandle);
                        this._cleanupRequest(correlationId, replyConsumer);
                        
                        console.log(`Response received for correlation ID: ${correlationId}`);
                        
                        // Return the response content (maintain backward compatibility)
                        if (typeof messageData.content === 'object') {
                            resolve(JSON.stringify(messageData.content));
                        } else {
                            resolve(messageData.content);
                        }
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
                            // Extract full message data
                            const messageData = this._extractMessageData(message);
                            const replyTo = message.getReplyTo();
                            const correlationId = message.getCorrelationId();

                            console.log(`Enhanced request received on topic: ${requestTopic}, correlation ID: ${correlationId}`);

                            if (replyTo && correlationId) {
                                // Process the request with full message data
                                let response;
                                try {
                                    response = await requestHandler(messageData.content, message, messageData.metadata);
                                } catch (handlerError) {
                                    console.error('Request handler error:', handlerError);
                                    response = { error: 'Internal server error', message: handlerError.message };
                                }

                                // Send response with enhanced options
                                await this._sendEnhancedResponse(replyTo, correlationId, response, messageData.metadata);
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
                        console.log(`Enhanced request handler set up for topic: ${requestTopic}`);
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
     * @param {Object} options.defaultMessageOptions - Default message options for all requests
     * @returns {Object} - Request-reply session object with optimized methods
     */
    createRequestReplySession(options = {}) {
        const {
            requestTopic,
            defaultTimeout = 5000,
            maxConcurrentRequests = 100,
            defaultMessageOptions = {}
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
             * Send a request using the optimized session with enhanced options
             */
            sendRequest: async (message, timeout = defaultTimeout, messageOptions = {}) => {
                if (activeRequests.size >= maxConcurrentRequests) {
                    throw new Error(`Maximum concurrent requests (${maxConcurrentRequests}) exceeded`);
                }

                const requestId = `${requestTopic}_${++requestCounter}_${Date.now()}`;
                activeRequests.set(requestId, Date.now());
                
                try {
                    const combinedOptions = { ...defaultMessageOptions, ...messageOptions };
                    const response = await this.sendRequest(requestTopic, message, timeout, combinedOptions);
                    return response;
                } finally {
                    activeRequests.delete(requestId);
                }
            },

            /**
             * Send a persistent request with high priority
             */
            sendPersistentRequest: async (message, timeout = defaultTimeout, messageOptions = {}) => {
                const enhancedOptions = {
                    ...defaultMessageOptions,
                    ...messageOptions,
                    persistent: true,
                    priority: 200,
                    dmqEligible: true
                };
                
                return this.sendRequest(requestTopic, message, timeout, enhancedOptions);
            },

            /**
             * Get session statistics
             */
            getStats: () => ({
                activeRequests: activeRequests.size,
                maxConcurrentRequests,
                totalRequestsSent: requestCounter,
                requestTopic,
                defaultTimeout,
                defaultMessageOptions
            }),

            /**
             * Close the request-reply session
             */
            close: () => {
                // Clean up any active requests
                for (const [requestId] of activeRequests) {
                    activeRequests.delete(requestId);
                }
                console.log(`Enhanced request-reply session closed for topic: ${requestTopic}`);
            }
        };
    }

    // ============= PRIVATE HELPER METHODS =============

    /**
     * Send an enhanced response message to the specified reply destination
     * 
     * @private
     * @param {Object} replyTo - Reply destination
     * @param {string} correlationId - Correlation ID from the original request
     * @param {string|Object} responseData - Response data
     * @param {Object} originalMetadata - Metadata from the original request
     */
    async _sendEnhancedResponse(replyTo, correlationId, responseData, originalMetadata = {}) {
        try {
            // Prepare response options based on original request
            const responseOptions = {
                correlationId: correlationId,
                messageType: 'RESPONSE',
                contentType: 'application/json'
            };

            // Inherit some properties from the original request if appropriate
            if (originalMetadata.priority !== undefined) {
                responseOptions.priority = originalMetadata.priority;
            }

            // Send response using the enhanced publish method
            await this.publishMessage(replyTo.getName(), responseData, responseOptions);
            
            console.log(`Enhanced response sent for correlation ID: ${correlationId}`);
        } catch (error) {
            console.error('Failed to send enhanced response:', error);
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