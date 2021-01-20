'use strict'

const { Kafka } = require('kafkajs');

const client = new Kafka({
    clientId: 'kafka-node-example',
    brokers: ['localhost:9092'],
});

module.exports = {
    /**
     * @param {String} topicName 
     * @param {Object} message
     * @description Método que envia para o tópico especificado o objeto enviado
     * @return {Promise<Object>} 
     */
    producer: async (topicName, message) => {
        // Criando producer
        const producer = client.producer();

        // Convertendo a mensagem para uma string
        const value = JSON.stringify(message);

        // Conectando o producer ao cluster kafka
        await producer.connect();

        // Enviando a mensagem para o tópico especificado
        return await producer.send({
            topic: topicName,
            messages: [{
                value,
            }],
        });
    },

    /**
     * @param {String} topicName 
     * @param {Object} message
     * @description Método que recebe as informações do consumer
     * @return {Promise<Object>} 
     */
    consumer: async (topicName, eachMessageFunction) => {
        // Criando consumer
        const consumer = client.consumer();

        // Conectando o consumer ao cluster kafka
        await consumer.connect();

        // Conectando o consumer ao tópico especificado
        await consumer.subscribe({
            topic: topicName,
        });

        // Executando consumer
        await consumer.run({
            eachMessage: eachMessageFunction,
        });
    },
}