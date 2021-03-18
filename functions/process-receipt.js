require('dotenv').config();

const amqp = require('amqplib');
const process = require('process');
const isEmpty = require('lodash.isempty');

const url = process.env.RABBITMQ_URL;

exports.handler = async () => {
    try {
        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();
        const queue = 'raw-data';

        await channel.assertQueue(queue, {durable: true});
        await channel.prefetch(1);

        console.log('[-__-] Waiting for messages in the %s queue', queue);

        let receipt = {};

        await channel.consume(queue, (data) => {
            const msg = JSON.parse(data.content)[0];

            console.log('[o_o] Consuming message as: ', data.fields.consumerTag);

            const products = mineProducts(msg);
            const storeInfo = mineStoreInfo(msg);
            const receiptNumber = msg.subArray[0]['Receipt_Number'];
            const cardNumber = msg.subArray[0]['CardNumber'];
            const paymentType = cardNumber ? 'card' : 'cash';
            const totalPrice = msg.subArray[0]['Total_Price'];
            const purchaseDate = msg.subArray[0]['Purchase_Date'];

            receipt = {
                products,
                storeInfo,
                receiptNumber,
                cardNumber,
                paymentType,
                totalPrice,
                purchaseDate,
            };

            channel.ack(data);
        }, {noAck: false});

        await channel.close();

        if (!isEmpty(receipt)) {
            // TODO check if queue is empty before sending data
            const channel2 = await conn.createChannel();
            const processedQueue = 'processed-data';
            await channel2.assertExchange('receipts', 'direct');
            await channel2.bindQueue(processedQueue, 'receipts', processedQueue);
            channel2.publish('receipts', processedQueue, Buffer.from(JSON.stringify(receipt)), { persistent: true });

            console.log('[o] The following message was successfully processed & sent to the %s queue:', processedQueue, receipt);

            await channel2.close();
        }

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
        };
    }
};

function mineProducts(data) {
    const products = [];

    data.subArray.forEach((receiptItem) => {
        const product = {
            productName : receiptItem['Prod_Name'],
            productDescription : receiptItem['Prod_Desc'],
            productPrice : receiptItem['Price'],
            productSupplierName : receiptItem['Supplier_Name'],
            productSupplierContact : receiptItem['Supplier_Contact'],
            productSupplierLocation : receiptItem['Supplier_Location'],
            productQuantity: receiptItem['Product_Quantity']
        };

        products.push(product);
    });

    return products;
}

function mineStoreInfo(data) {
    const receiptItem = data.subArray[0];
    
    return {
        storeName : receiptItem['Store_Name'],
        storeContact: receiptItem['Store_Contact'],
        storeStreetName: receiptItem['Street_Name'],
        storeCity: receiptItem['City'],
        storeZipCode: receiptItem['Zip_Code'],
        storeProvince: receiptItem['Province'],
    };
}
