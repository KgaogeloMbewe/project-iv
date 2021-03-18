require('dotenv').config();

const amqp = require('amqplib');
const process = require('process');

const url = process.env.RABBITMQ_URL;

const mysql = require('mysql2/promise');

exports.handler = async () => {
    try {
        const conn = await amqp.connect(url);
        const channel = await conn.createChannel();
        const queue = 'processed-data';

        await channel.assertQueue(queue, {durable: true});
        await channel.prefetch(1);

        console.log('[*_o] Waiting for messages in the %s. queue', queue);
        let msg = {};

        await channel.consume(queue, async (data) => {
            msg = JSON.parse(data.content);

            channel.ack(data);
        }, {noAck: false});

        try {
            const dbConn = await mysql.createConnection({
                user: 'db-user-1',
                password: 'db_user-1',
                database: 'project_iv',
                host: '127.0.0.1',
                port: '5431',
            });
    
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
        }

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
        };
    }
};

async function getPaymentOption (conn, data) {
    const paymentDesc = (data.paymentInfo.length > 0 && data.paymentInfo.cardNumber) ? 'card' : 'cash';
    const q = 'SELECT `pay_option_id` FROM `payment_options` where `pay_option_desc` = ? ';
    const qResult = await conn.query(q, [paymentDesc]);

    if (qResult[0].length && qResult[0][0]) {
        return qResult[0][0].pay_option_id;
    }

    const i = 'INSERT INTO `payment_options` (`pay_option_desc`) VALUES (?)';
    const iResult = await conn.execute(i, [paymentDesc]);

    return iResult[0].insertId;
}

async function getStore (conn, data) {
    const q = 'SELECT `store_id` FROM `store_details` WHERE `store_name` = ? and `store_contact` = ?';
    const qResult = await conn.query(q, [data.storeInfo.storeName, data.storeInfo.storeContact]);

    if (qResult[0].length && qResult[0][0]) {
        return qResult[0][0].store_id;
    }

    const i = 'INSERT INTO `store_details`\
     (`store_name`, `store_street_name`, `store_city`, `store_province`, `store_zip_code`, `store_contact`)\
      VALUES\
       (?, ?, ?, ?, ?, ?)';
    const iResult = await conn.execute(i, [
        data.storeInfo.storeName, 
        data.storeInfo.storeStreetName,
        data.storeInfo.storeCity, 
        data.storeInfo.storeProvince, 
        data.storeInfo.storeZipCode,
        data.storeInfo.storeContact
    ]);

    return iResult[0].insertId;
}

async function getSupplier (conn, data) {
    const q = 'SELECT `supplier_id` FROM `suppliers` WHERE `supplier_name` = ? and `supplier_contact` = ?';
    const qResult = await conn.query(q, [data.productSupplierName, data.productSupplierContact]);

    if (qResult[0].length && qResult[0][0]) {
        return qResult[0][0].supplier_id;
    }

    const i = 'INSERT INTO `suppliers`\
     (`supplier_name`, `supplier_contact`, `supplier_location`)\
      VALUES\
       (?, ?, ?)';
    const iResult = await conn.execute(i, [
        data.productSupplierName, 
        data.productSupplierContact,
        data.productSupplierLocation
    ]);

    return iResult[0].insertId;
}

async function getProduct (conn, data, storeId) {
    const q = 'SELECT `product_id` FROM `products` WHERE `product_name` = ? and `store_id` = ?';
    const qResult = await conn.query(q, [data.productName, storeId]);

    if (qResult[0].length && qResult[0][0]) {
        return qResult[0][0].product_id;
    }

    const i = 'INSERT INTO `products`\
     (`product_name`, `product_desc`, `product_price`, `store_id`)\
      VALUES\
       (?, ?, ?, ?)';
    const iResult = await conn.execute(i, [
        data.productName, 
        data.productDescription,
        parseFloat(data.productPrice), 
        storeId,
    ]);

    return iResult[0].insertId;
}

async function createReceipt(conn, data) {
    const i = 'INSERT INTO `receipts`\
     (`receipt_number`, `product_id`, `store_id`, `pay_option_id`, `supplier_id`, `price`)\
      VALUES\
       (?, ?, ?, ?, ?, ?)';
    const iResult = await conn.execute(i, [
        data.receiptNumber, 
        data.productId,
        data.storeId, 
        data.payOptionId, 
        data.supplierId,
        data.price,
    ]);

    return iResult[0].insertId;
}
