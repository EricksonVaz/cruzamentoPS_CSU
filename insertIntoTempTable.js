var parser = new (require('simple-excel-to-json').XlsParser)();
var arrayPaginate = require('array-paginate');
console.log("start load excel...")
var ListagemGeralPS = parser.parseXls2Json('./ListagemGeralPSJunho2024.xlsx');

const sqlServer = require('mssql');

const csuBdConnection = {
    user:"CSUQCLogin",
    password: "@!Q5@2C*A&#",
    server: "csu.cv",
    port: +("1334"),
    database: "CSU_APIs_Feedback_Loops",
    encrypt:false,
    requestTimeout:3600000
}

let arrListagemGeralPS = ListagemGeralPS[0].map(el=>`('${el.N}','${el.Nome}','${el.BI}','${el.Residencia}','${el.Concelho}')`);

let arrListagemGeralPSPaginated = arrayPaginate(arrListagemGeralPS,1,1000);

async function execute(){
    console.log("Executando save na base de dados")
    let reult = await savePSFeedback(arrListagemGeralPSPaginated.docs)
    console.log("Feito",reult)
}
 execute();


//console.log(arrListagemGeralPSPaginated.docs.join(","))
            

// let {totalPages=1} = arrListagemGeralPSPaginated;

// console.log(totalPages,listToSave.length)

// let querySQL = ""
// for (let currentPage = 1; currentPage <= totalPages; currentPage++) {
//     let newArrListagemGeralPS= [];
//     if(currentPage==1) newArrListagemGeralPS = arrListagemGeralPSPaginated.docs;
//     else newArrListagemGeralPS = arrayPaginate(arrListagemGeralPS,currentPage,1000).docs;


//     console.log(currentPage,newArrListagemGeralPS.length)
//     querySQL = await savePSFeedback(newArrListagemGeralPS);

// }

async function savePSFeedback(valuesToSave){
    let pool = await sqlServer.connect(csuBdConnection);

    let querySQL = `
        INSERT INTO [CSU_APIs_Feedback_Loops].[dbo].[CSU_PS_Julho_202]
            ([N]
            ,[Nome]
            ,[BI]
            ,[Residencia]
            ,[Concelho])
        VALUES
            ${valuesToSave.join(",")}
    `;

    try{
        const result = await pool.request()
        .query(querySQL);

        pool.close();
    }catch(err){
        
    }finally{
            return querySQL;
    }

    
}