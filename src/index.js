'use strict'

const { producer, consumer } = require('./config/kafka');

const handleProducer = async () => {
    // Definindo objeto de informação de usuário
    const userProfile = ({
        name: 'Alef Felix',
        email: 'alef.developerweb@gmail.com',
    });

    // Log de envio
    console.log(`${userProfile.name} enviado para o tópico kafka...`);

    // Enviando os dados de usuário para o Kakfa
    return await producer('users', userProfile);
}

const handleConsumer = async () => {
    // Chamando consumer para ler os dados emitidos pelo tópico
    await consumer('users', async ({ topic, partition, message }) => {
        // Retornando a partição, identificador e valor da mensagem recebida
        console.log({
            partition,
            offset: message.offset,
            value: message.value.toString(),
        });
    });
}

// Caso queira executar o producer para enviar uma mensagem
handleProducer().catch(console.error);

// Caso queira executar o consumer para ler as mensagens de um tópico
handleConsumer().catch(console.error);