var parser = new (require('simple-excel-to-json').XlsParser)();
console.log("start load excel...")
var ListagemGeralPS = parser.parseXls2Json('./cruzamento_PS_CNCSU.xlsx');
var json2xls = require('json2xls');
const sqlServer = require('mssql');

const csuBdConnection = {
    user:"CSUQCLogin",
    password: "@!Q5@2C*A&#",
    server: "csu.cv,1334",
    database: "CSU_PRD_GEO",
    encrypt:false,
    requestTimeout:3600000
}

async function getConnectionPool(connectioInfoOBJ){
    const {user,server,database,password,encrypt,requestTimeout} = connectioInfoOBJ;
    return await sqlServer.connect(`Server=${server};Database=${database};User Id=${user};Password=${password};Encrypt=${encrypt};request timeout=${requestTimeout}`)
}
var fs = require("fs");
console.log("finish load excel...");

console.log("start iteration in excel...")
let arrCSUFinded = ListagemGeralPS[0].filter(el=>(el?.OBJ??"").trim()=="Encontrado");
console.log("finish iteration in excel...")

let nimFinded = [];

console.log("start loop 1");
let totalNamesPS = arrCSUFinded.length;
let currentTotal = 0
for (const {N,OBJ,BI,Nome,Residencia,Concelho,NIM,NomeCompleto,DocID} of arrCSUFinded) {
    if(!(nimFinded.includes((NIM??"").trim()))){
        nimFinded.push((NIM??"").trim());
    }
    currentTotal++;

    console.log("% completo",((currentTotal/totalNamesPS)*100))
}
console.log("finish loop");

let NIMListString = nimFinded.map(el=>`'${el}'`).join(",");

async function getListBenefitPS(){
    let querySQL = `
        SELECT 
            parentglobalid CSUGlobalID
            ,'m' Type
            ,b.IAF201 NIA
            ,[MAF3011] NIM
            ,18 BenefitID
            ,getdate() CreationDate
            ,upper(b.L102) Ilha
            ,b.L103 Concelho
            ,b.L107 Zona
            ,b.L108 ReferenciaMorada
            ,ltrim(rtrim(IAF2022)) as Representante
            ,[MAF302] Membro
            ,[CSU_PRD_GEO].[sde].[fn_Idade](MAF308) AS Idade
            ,CASE MAF307 WHEN '1' THEN 'Masculino' WHEN '2' THEN 'Feminino' END AS 'Sexo'
            ,MAF3142 DocID
            ,MAF315 NIF
            ,IAF203 AS Contacto
            ,I.GRUPO Grupo
            ,null BeneficiaryAmountPay
            ,null BeneficiaryDiscount
            ,b.estadoficha CSUStatus
            ,GETDATE() BenefitReceiptDate
            ,1 CreatedBy
            ,NULL OBS 
            ,1 Status
            ,b.GDB_TO_DATE Modificacao
        FROM [CSU_PRD_GEO].[sde].[CSU_MEMBROS] a LEFT JOIN [CSU_PRD_GEO].[sde].[CSU_AGREGADOS] b
        ON a.parentglobalid = b.globalid
        LEFT JOIN [CSU_PRD_GEO].[sde].[CSU_INDICADOR_FOCALIZACAO_Unico] I ON (b.GLOBALID = I.ID)
        WHERE a.GDB_TO_DATE = '9999-12-31 23:59:59.0000000'
        AND (
            b.GDB_TO_DATE = '9999-12-31 23:59:59.0000000' OR b.estadoficha in ('Anulado','Actualizado','Levantamento', 'Em espera')
        )
        AND MAF3011 in (${NIMListString})
        ORDER BY NIM, Modificacao DESC
    `;

    let pool = await getConnectionPool(csuBdConnection);

    const result = await pool.request()
    .query(querySQL);

    pool.close();

    return result['recordset'] ?? [];
}

let listBenefitPSFeedbackLoop = [];

let checkArr = [];
let objCSUNoDuplicates = [];

async function loadFeedbackData(){
    listBenefitPSFeedbackLoop = await getListBenefitPS();

    totalNamesPS = listBenefitPSFeedbackLoop.length;
    currentTotal = 0;
    console.log("start loop 2");
    for (const obj of listBenefitPSFeedbackLoop) {
        if(!(checkArr.includes((obj?.NIM??"").trim()))){
            checkArr.push((obj?.NIM??"").trim())

            objCSUNoDuplicates.push(obj)
        }

        currentTotal++;

        console.log("% completo",((currentTotal/totalNamesPS)*100))
    }
    console.log("finish loop 2");

    console.log("start create excel")

    var xlsToLoad = json2xls(objCSUNoDuplicates);

    fs.writeFileSync(`./Final_FeedbackLoopPS_CNCSU.xlsx`, xlsToLoad, 'binary');

    console.log("finish create excel")
}

loadFeedbackData();