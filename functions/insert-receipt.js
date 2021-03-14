require('dotenv').config();

const amqp = require('amqplib');
const process = require('process');

const url = process.env.RABBITMQ_URL;

exports.handler = async () => {
    try {
        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();
        const queue = 'processed-data'; //TODO check if needs rename

        await channel.assertQueue(queue, {durable: true});
        // await channel.prefetch(1); TODO uncomment before processing data

        console.log('[*_o] Waiting for messages in the %s. queue', queue);

        const receipt = await channel.consume(queue, null, {noAck: false});

        //TODO Add logic to insert the processed receipt into data warehouse

        await channel.close();
        await conn.close();

        return {
            statusCode: 200,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({'msg': 'Successful'}),
        };
    } catch (error) {
        console.log(error);

        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({'error': 'An Error occurred'}),
        }
    }
};