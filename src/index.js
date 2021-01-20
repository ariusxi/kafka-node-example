'use strict'

const { producer } = require('./config/kafka');

const run = async () => {
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

run().catch(console.error);