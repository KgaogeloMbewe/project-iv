const amqp = require('amqplib');

const url = "amqps://xrgawglz:9bwYNG_BuoeVu10TBSY70fW6trZdHVVp@rattlesnake.rmq.cloudamqp.com/xrgawglz";

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
        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();

        //TODO: Add logic to check if event.body has content

        const queue = 'raw-data';
        const msg = event.body;
        await channel.assertQueue(queue, { durable: true });
        channel.sendToQueue(queue, Buffer.from(msg), { persistent: true });

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
