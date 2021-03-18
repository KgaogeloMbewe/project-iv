require('dotenv').config();

const process = require('process');
const amqp = require('amqplib');
const isPlainObject = require('lodash.isplainobject');
const isEmpty = require('lodash.isempty');

const url = process.env.RABBITMQ_URL;

exports.handler = async (event) => {
    if (event.httpMethod !== 'POST') {
        console.error('[x] The HTTP method is not a POST');

        return {
            statusCode: 405,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({'error': 'Method not allowed'}),
        };
    }

    try {
        const msg = JSON.parse(event.body);

        if (!isPlainObject(msg) || isEmpty(msg)) {
            throw new SyntaxError('Data sent through is not a PlainObject');
        }

        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();

        const queue = 'raw-data';
        await channel.assertExchange('receipts', 'direct');
        await channel.bindQueue(queue, 'receipts', queue);
        channel.publish('receipts', queue, Buffer.from(JSON.stringify([msg])), { persistent: true });

        console.log('[o] The following message was successfully sent to the %s queue:', queue, msg);

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
        console.log(`(>_<") A [${error.name}] occurred on line ${error.lineNumber}: ${error.message} | ${error.stack}`);

        if (error instanceof SyntaxError) {
            //Probably JSON sent to the API is flop
            return {
                statusCode: 400,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({'error': 'An Error occurred. The data sent through is not a properly structured JSON object'}),
            };
        } else {
            return {
                statusCode: 500,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({'error': 'An Error occurred. Please try again later.'}),
            };
        }
    }
};
