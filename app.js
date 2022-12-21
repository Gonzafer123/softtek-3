// import fetch from 'node-fetch';


// import express, { json } from 'express';
// const app = express();
// import morgan from 'morgan';
// import cors from 'cors';

//fetch
//const response = await fetch('https://lakeg3.blob.core.windows.net/datos/tabla.json');
//const data = await response.json();

//console.log(data);

const {
    AzureStorageDataLake,
    DataLakeServiceClient,
    StorageSharedKeyCredential
    } = require("@azure/storage-file-datalake");

let accountName = "storageg31252914003"
let accountKey =  "yPQefaTq0/KzniO/W9agVj1tqkbZ1AnjE6VmObHuhM3Foe3mictym2baoNm88tn1m2Fgp3HrMkid+AStQvJv9Q=="

// funcion para leer data 
async function GetDataLakeServiceClient(content) {
    
  



    const sharedKeyCredential =
       new StorageSharedKeyCredential(accountName, accountKey);
  
    const datalakeServiceClient = new DataLakeServiceClient(
        `https://${accountName}.dfs.core.windows.net`, sharedKeyCredential);

 //contenedor nombre
        const fileSystemName = "entrada";

        const fileSystemClient = datalakeServiceClient.getFileSystemClient(fileSystemName);
      
try {
    
    const fileClient = fileSystemClient.getFileClient("DatosIncertar.json");      
   
    const downloadResponse = await fileClient.read();     
    let downloaded =  await streamToBuffer(downloadResponse.readableStreamBody) ;  
    let downloadedString = await JSON.parse(downloaded)  
     


//poruva

//console.log(content)
//content=  await JSON.stringify(content)
//await fileClient.create();
//await fileClient.append(content, 0, content.length );
//await fileClient.flush(content.length);



     downloadedString.push(content)


     downloadedString=  await JSON.stringify(downloadedString)

     console.log(downloadedString)
 


   
     async function streamToBuffer(readableStream) {
       return new Promise((resolve, reject) => {
         const chunks = [];
         readableStream.on("data", (data) => {
           chunks.push(data instanceof Buffer ? data : Buffer.from(data));
         });
         readableStream.on("end", () => {
           resolve(Buffer.concat(chunks));
         });
         readableStream.on("error", reject);
       });
     }



     await fileClient.create();
      await fileClient.append(downloadedString, 0, downloadedString.length );
    await fileClient.flush(downloadedString.length);




} catch (error) {
    

        // directorio nombre
      //  const directoryClient = fileSystemClient.getDirectoryClient("my-directory");
      content=  await JSON.stringify(content)
      const fileClient = fileSystemClient.getFileClient("DatosIncertar.json");      
   
      await fileClient.create();
      await fileClient.append(content, 0, content.length );
    await fileClient.flush(content.length);

  
    //return datalakeServiceClient;

}
  }


/* 
  GetDataLakeServiceClient(
  [{Producto:"Casco deportivo: 100, rojo",Categoria:"Accesorio",SubCategoria:"Casco",Sucursal:9,Stock:20.0},
  {Producto:"Nuevo",Categoria:"Accesorio",SubCategoria:"Casco",Sucursal:0,Stock:30.0},
  {Producto:"Nuevo1",Categoria:"Nueva",SubCategoria:"Nueva",Sucursal:0,Stock:30.0},
  {Producto:"Casco deportivo: 100, rojo",Categoria:"Accesorio",SubCategoria:"Casco",Sucursal:9,Stock:-2.0}]
  ) */



const express = require('express');
const app = express();
const morgan=require('morgan');
const cors = require('cors');

const data = require('./tabla.json')
//const {Categoria, Cod_Categoria,Cod_SubCategoria, SubCategoria} = data




const arraySubCategoria = data.map(e => e.SubCategoria)
const arrayCatFiltered = data.filter(({ SubCategoria }, index) => !arraySubCategoria.includes(SubCategoria, index + 1))

let listaFiltrada = arrayCatFiltered.map(e => {
    let { Categoria, Cod_Categoria, Cod_SubCategoria, SubCategoria } = e
    return ({ Categoria, Cod_Categoria, Cod_SubCategoria, SubCategoria })
})

//console.log(listaFiltrada)


/* 
{
    categoria : "acesorio" , 
    Cod_Categoria : "4"
    SubCategoria :
} */
/* Producto
: 
"Casco deportivo: 100, negro"
Stock
: 
"3.0"
SubCategoria
: 
"Casco"
Sucursal
: 
"Sucursal 08" */

//Configuraciones
app.set('port', process.env.PORT || 3000);
app.set('json spaces', 2)


//Middleware
app.use(cors({ origin: true, credentials: true }));
app.use(morgan('dev'));
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

//Nuestro primer WS Get
app.get('/', (req, res) => {
    res.json(listaFiltrada);
});

app.get('/cliente', (req, res) => {
    const { categoria, codSubCategoria, producto } = req.query
    if (producto) {
        let resp = buscarPrdocuto(categoria, producto, codSubCategoria)
        if (resp== false){
            console.log("no econtro igual")
        let  resulProdVacio =  devolverPrdocutos(categoria, codSubCategoria)
        res.json(resulProdVacio); 
        }else{
            res.json(resp);
        }
        
    } else {
        let  resulProd =  devolverPrdocutos(categoria,codSubCategoria)
        res.json(resulProd); 

    }
});

app.post('/proveedor', (req, res) => {
    const { categoria, codSubCategoria, producto } = req.body
   //revisrar de hacer el fecth 
   // enviar agredar datos
   GetDataLakeServiceClient(content)

    
});




//Iniciando el servidor
app.listen(app.get('port'), () => {
    console.log(`Server listening on port ${app.get('port')}`);
});


const buscarPrdocuto = (cat, prod, subCat) => {
    console.log("-->",cat, subCat, prod)
   
     //   let productosExistentes = []
        prod = prod.trim().toLowerCase()
        cat = cat.trim().toLowerCase()
      

        //const result = data.filter(({ Producto }) => Producto.toLowerCase() === prod);
        
        const result = data.map(( e ) => {
            let prodExiste = (e.Producto.toLowerCase() == prod) && (e.Cod_SubCategoria.toLowerCase() == subCat
            && (e.Categoria.toLowerCase().includes(cat)))
            if(prodExiste){
               return e 
            }
        });
        let filtrado = result.filter((e)=>
            e != undefined
        );
        
       
        console.log("encontro producto",filtrado)
        return filtrado

  

}


const devolverPrdocutos = (cat, subCat) => {
                
        cat = cat.trim().toLowerCase()      
        let  resulProd = data.filter(({ Categoria, Cod_SubCategoria }) => (Categoria.toLowerCase() === cat) & (Cod_SubCategoria === subCat));
        console.log(resulProd)
          return resulProd

    }



/*     let downloadedString =
     [
        {
          Producto: 'Casco deportivo: 100, rojo',
          Cod_Producto: '212',
          Categoria: 'Accesorio',
          Cod_Categoria: '4',
          SubCategoria: 'Casco',
          Cod_SubCategoria: '31',
          Sucursal: 'Sucursal 09',
          Stock: '9.0'
        },
        {
          Producto: 'Casco deportivo: 100, rojo',
          Cod_Producto: '212',
          Categoria: 'Accesorio',
          Cod_Categoria: '4',
          SubCategoria: 'Casco',
          Cod_SubCategoria: '31',
          Sucursal: 'Sucursal 09',
          Stock: '9.0'
        }
      ] */