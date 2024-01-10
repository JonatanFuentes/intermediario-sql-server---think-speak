const axios = require('axios');
const sql = require('mssql');

//Credenciales BD Azure
const config = {
    
    options: {
        encrypt: true
    }
};

//Definir constantes para conteo de durancion detencion
let contando = false;
let inicioConteo = null;
let duracionAcumulada = 0;

//Obtenemos datos de la bd Thingpspeak mediante la API
async function obtenerDatosThingSpeak() {
    try {
        const response = await axios.get('https://api.thingspeak.com&results=1');
        return response.data.feeds;
    } catch (error) {
        console.error('Error al obtener datos de ThingSpeak:', error);
        throw error;
    }
}
//Los datos de thing speak se formatean primero para ingresarse en la bd, como fecha y  hora
async function insertarDatosSQLServer(datos) {
    try {
        await sql.connect(config);
        const request = new sql.Request();
        
        for (let dato of datos) {
            const fechaHora = dato.created_at.split("T"); 
            let fecha = fechaHora[0]; 
            let hora = fechaHora[1]; 
            
            // Separar las partes de la hora
            const [hh, mm, ss] = hora.split(":");
            
            // Restar 3 horas
            let nuevaHora = parseInt(hh, 10) - 3;
            
            //  comprobar que la hora no sea negativa y ajustar el día si es necesario
            if (nuevaHora < 0) {
                nuevaHora += 24;
                fecha = fecha.split("-").map((part, index) => {
                    if (index === 2) { 
                        return String(parseInt(part, 10) - 1).padStart(2, '0');
                    }
                    return part;
                }).join("-");
            }
            
            // Componer la nueva hora en formato HH:MM:SS
            hora = `${String(nuevaHora).padStart(2, '0')}:${mm}:${ss}`;
            
//calcular y acumular la duracion de un evento, luego del calculo se insertan los dastos en la tabla Evento 
//Lo que calcula es solamente la fecha, hora y tiempo de detencion provenientes del Arduino, datos como IdSensor,Planta y Sector se configuran en la Query
if (dato.field1 == 1 && !contando) { 
    contando = true;
    inicioConteo = new Date(fechaHora.join(" "));
} else if (dato.field1 == 0 && contando) {
    contando = false;
    const finConteo = new Date(fechaHora.join(" "));
    duracionAcumulada += (finConteo - inicioConteo) / (1000 * 60); 
    const horas = Math.floor(duracionAcumulada / 60);
    const minutos = Math.floor(duracionAcumulada % 60);
    const segundos = Math.floor((duracionAcumulada - Math.floor(duracionAcumulada)) * 60);
    const tiempoFormateado = `${String(horas).padStart(2, '0')}:${String(minutos).padStart(2, '0')}:${String(segundos).padStart(2, '0')}`;
    
    const query = `
    INSERT INTO Evento (IdSensor, Fecha, Hora, DuracionDetencion, IdSector, Idplanta, ValorSenal)
    VALUES ('30', '${fecha}', '${hora}', '${tiempoFormateado}', '1', '10', 0);
    `;
    await request.query(query);
    duracionAcumulada = 0; 
} else if (contando) {
    const finConteo = new Date(fechaHora.join(" "));
    duracionAcumulada += (finConteo - inicioConteo) / (1000 * 60); 
    inicioConteo = finConteo; 
}

            const query = `
            INSERT INTO Evento (IdSensor, Fecha, Hora, DuracionDetencion, IdSector, Idplanta, ValorSenal)
            VALUES ('30', '${fecha}', '${hora}', '00:00:00', '1', '10', ${dato.field1});
            `;
            
            await request.query(query);
        }
        
        await sql.close();
    } catch (error) {
        console.error('Error al insertar datos en SQL Server:', error);
        throw error;
    }
}
//Funcion que demuestra mensajes en Consola
async function main() {
    try {
        console.log('Iniciando proceso de inserción de datos...');
        await ejecutarInsercion();
        
        setInterval(async () => {
            await ejecutarInsercion();
        }, 15000);  
    } catch (error) {
        console.error('Error en el proceso principal:', error);
    }
}
//Ocupa las funciones para obtener datos e insertar para insertar los datos en SQL Server
async function ejecutarInsercion() {
    try {
        const datosThingSpeak = await obtenerDatosThingSpeak();
        await insertarDatosSQLServer(datosThingSpeak);
        console.log('Datos insertados correctamente en SQL Server.');
    } catch (error) {
        console.error('Error al ejecutar inserción:', error);
    }
}

main();
