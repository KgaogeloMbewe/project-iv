const amqp = require('amqplib');

const url = "amqps://xrgawglz:9bwYNG_BuoeVu10TBSY70fW6trZdHVVp@rattlesnake.rmq.cloudamqp.com/xrgawglz";

exports.handler = async () => {

    try {
        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();

        const queue = 'hello';
        const msg = 'Hello World';
        channel.assertQueue(queue, { durable: false });
        channel.sendToQueue(queue, Buffer.from(msg));

        return {
            statusCode: 200,
            body: "Hello World",
        };
    } catch (error) {
        console.log(error);

        return {
            statusCode: 500,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify('An Error occurred'),
        }
    }
};
