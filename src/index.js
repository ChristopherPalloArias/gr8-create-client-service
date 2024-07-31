import express from 'express';
import cors from 'cors';
import amqp from 'amqplib';
import swaggerUi from 'swagger-ui-express';
import swaggerJsDoc from 'swagger-jsdoc';
import AWS from 'aws-sdk';

// AWS region and Lambda function configuration
const region = "us-east-2";
const lambdaFunctionName = "fetchSecretsFunction_gr8";

// Function to invoke Lambda and fetch secrets
async function getSecretFromLambda() {
  const lambda = new AWS.Lambda({ region: region });
  const params = {
    FunctionName: lambdaFunctionName,
  };

  try {
    const response = await lambda.invoke(params).promise();
    const payload = JSON.parse(response.Payload);
    if (payload.errorMessage) {
      throw new Error(payload.errorMessage);
    }
    const body = JSON.parse(payload.body);
    return JSON.parse(body.secret);
  } catch (error) {
    console.error('Error invoking Lambda function:', error);
    throw error;
  }
}

// Function to start the service
async function startService() {
  let secrets;
  try {
    secrets = await getSecretFromLambda();
  } catch (error) {
    console.error(`Error starting service: ${error}`);
    return;
  }

  const app = express();
  const port = 8094;

  app.use(cors());
  app.use(express.json());

  // Configure AWS DynamoDB
  AWS.config.update({
    region: region,
    accessKeyId: secrets.AWS_ACCESS_KEY_ID,
    secretAccessKey: secrets.AWS_SECRET_ACCESS_KEY,
  });

  const dynamoDB = new AWS.DynamoDB.DocumentClient();

  // Swagger setup
  const swaggerOptions = {
    swaggerDefinition: {
      openapi: '3.0.0',
      info: {
        title: 'Client Service API',
        version: '1.0.0',
        description: 'API for managing clients',
      },
    },
    apis: ['./src/index.js'],
  };

  const swaggerDocs = swaggerJsDoc(swaggerOptions);
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocs));

  // Connect to RabbitMQ
  let channel;
  async function connectRabbitMQ() {
    try {
      const connection = await amqp.connect('amqp://3.136.72.14:5672/');
      channel = await connection.createChannel();
      await channel.assertQueue('client-events', { durable: true });
      console.log('Connected to RabbitMQ');
    } catch (error) {
      console.error('Error connecting to RabbitMQ:', error);
    }
  }

  // Publish event to RabbitMQ
  const publishEvent = async (eventType, data) => {
    const event = { eventType, data };
    try {
      if (channel) {
        channel.sendToQueue('client-events', Buffer.from(JSON.stringify(event)), { persistent: true });
        console.log('Event published to RabbitMQ:', event);
      } else {
        console.error('Channel is not initialized');
      }
    } catch (error) {
      console.error('Error publishing event to RabbitMQ:', error);
    }
  };

  await connectRabbitMQ();

  /**
   * @swagger
   * /clients:
   *   post:
   *     summary: Create a new client
   *     description: Create a new client with required details
   *     requestBody:
   *       description: Client object that needs to be created
   *       required: true
   *       content:
   *         application/json:
   *           schema:
   *             type: object
   *             properties:
   *               ci:
   *                 type: string
   *                 example: "1726647066"
   *               firstName:
   *                 type: string
   *                 example: "Christopher"
   *               lastName:
   *                 type: string
   *                 example: "Pallo"
   *               phone:
   *                 type: string
   *                 example: "0995312828"
   *               address:
   *                 type: string
   *                 example: "Condado"
   *     responses:
   *       201:
   *         description: Client created
   *         content:
   *           application/json:
   *             schema:
   *               type: object
   *               properties:
   *                 ci:
   *                   type: string
   *                   example: "1726647066"
   *                 firstName:
   *                   type: string
   *                   example: "Christopher"
   *                 lastName:
   *                   type: string
   *                   example: "Pallo"
   *                 phone:
   *                   type: string
   *                   example: "0995312828"
   *                 address:
   *                   type: string
   *                   example: "Condado"
   *       500:
   *         description: Error creating client
   */
  app.post('/clients', async (req, res) => {
    const { ci, firstName, lastName, phone, address } = req.body;
    console.log('Received request to create client:', req.body);

    try {
      // Save client to DynamoDB
      const params = {
        TableName: 'Clients_gr8',
        Item: {
          ci,
          firstName,
          lastName,
          phone,
          address,
        },
      };

      await dynamoDB.put(params).promise();

      // Publish client created event to RabbitMQ
      await publishEvent('ClientCreated', { ci, firstName, lastName, phone, address });

      res.status(201).send({ ci, firstName, lastName, phone, address });
    } catch (error) {
      console.error('Error creating client:', error);
      res.status(500).send({ message: 'Error creating client', error: error });
    }
  });

  app.get('/', (req, res) => {
    res.send('Client Service Running');
  });

  app.listen(port, () => {
    console.log(`Client service listening at http://localhost:${port}`);
  });
}

startService();
