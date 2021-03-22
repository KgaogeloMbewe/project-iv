require('dotenv').config();

const amqp = require('amqplib');
const process = require('process');
const { connectDb, createReceipt, getPaymentOption, getProduct, getStore, getSupplier } = require('../util/db-util');

const url = process.env.RABBITMQ_URL;

exports.handler = async () => {
    try {
        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();
        const queue = 'processed-data';
        let dbConn = null;

        await channel.assertQueue(queue, {durable: true});
        await channel.prefetch(1);

        console.log('[*_o] Waiting for messages in the %s. queue', queue);
        let msg = {};

        try {
            dbConn = await connectDb();
        } catch (error) {
            console.log('[>_<] unable to connect to DB');

            return {
                statusCode: 500,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({'error': 'An Error occurred'}),
            };
        }

        await channel.consume(queue, async (data) => {
            msg = JSON.parse(data.content);

            console.log('[o_o] Consuming processed message as: ', data.fields.consumerTag);

            // channel.ack(data);
        }, {noAck: false});

        await channel.close();
        await conn.close();

        try {
            for (const product of msg.products) {
                const payOptionId = await getPaymentOption(dbConn, msg);
                const storeId = await getStore(dbConn, msg);
                const productId = await getProduct(dbConn, product, storeId);
                const supplierId = await getSupplier(dbConn, product);
    
                const receipt = {
                    receiptNumber: msg.receiptNumber,
                    price: parseFloat(product.productPrice),
                    productQuantity: product.productQuantity,
                    purchaseDate: msg.purchaseDate,
                    payOptionId,
                    productId,
                    storeId,
                    supplierId
                };
    
                console.log('receipt generated:', receipt);

                const receiptId = await createReceipt(dbConn, receipt);

                console.log('receipt inserted at row ', receiptId);
            }

            await dbConn.end();
        } catch (error) {
            console.log('An error occurred while performing some DB operation: ', error);

            return {
                statusCode: 500,
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({'error': 'An Error occurred'}),
            };
        }

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
        };
    }
};
